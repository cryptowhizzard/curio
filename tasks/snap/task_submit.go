package snap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
    	"net/http"
    	"io/ioutil"
    	"sync"
    	"time"
	"strconv"

	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"go.uber.org/multierr"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	miner13 "github.com/filecoin-project/go-state-types/builtin/v13/miner"
	verifregtypes9 "github.com/filecoin-project/go-state-types/builtin/v9/verifreg"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/go-state-types/exitcode"

	"github.com/filecoin-project/curio/deps/config"
	"github.com/filecoin-project/curio/harmony/harmonydb"
	"github.com/filecoin-project/curio/harmony/harmonytask"
	"github.com/filecoin-project/curio/harmony/resources"
	"github.com/filecoin-project/curio/lib/curiochain"
	"github.com/filecoin-project/curio/lib/multictladdr"
	"github.com/filecoin-project/curio/lib/passcall"
	"github.com/filecoin-project/curio/tasks/message"
	"github.com/filecoin-project/curio/tasks/seal"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/types"
)

var log = logging.Logger("update")

var ImmutableSubmitGate = abi.ChainEpoch(2) // don't submit more than 2 minutes before the deadline becomes immutable

type SubmitTaskNodeAPI interface {
	StateSectorPartition(ctx context.Context, maddr address.Address, sectorNumber abi.SectorNumber, tsk types.TipSetKey) (*miner.SectorLocation, error)
	StateGetAllocation(ctx context.Context, clientAddr address.Address, allocationId verifregtypes9.AllocationId, tsk types.TipSetKey) (*verifregtypes9.Allocation, error)
	ChainHead(ctx context.Context) (*types.TipSet, error)

	WalletBalance(context.Context, address.Address) (types.BigInt, error)
	WalletHas(context.Context, address.Address) (bool, error)
	StateAccountKey(context.Context, address.Address, types.TipSetKey) (address.Address, error)
	StateLookupID(context.Context, address.Address, types.TipSetKey) (address.Address, error)
	StateSectorGetInfo(ctx context.Context, maddr address.Address, sectorNumber abi.SectorNumber, tsk types.TipSetKey) (*miner.SectorOnChainInfo, error)

	StateMinerInfo(context.Context, address.Address, types.TipSetKey) (api.MinerInfo, error)
	StateMinerAvailableBalance(context.Context, address.Address, types.TipSetKey) (big.Int, error)
	StateMinerInitialPledgeForSector(ctx context.Context, sectorDuration abi.ChainEpoch, sectorSize abi.SectorSize, verifiedSize uint64, tsk types.TipSetKey) (types.BigInt, error)
	StateGetActor(ctx context.Context, actor address.Address, tsk types.TipSetKey) (*types.Actor, error)
	StateVMCirculatingSupplyInternal(ctx context.Context, tsk types.TipSetKey) (api.CirculatingSupply, error)

	StateMinerProvingDeadline(context.Context, address.Address, types.TipSetKey) (*dline.Info, error)
}

type submitConfig struct {
	maxFee                     types.FIL
	RequireActivationSuccess   bool
	RequireNotificationSuccess bool
	CollateralFromMinerBalance bool
	DisableCollateralFallback  bool
}

type MpoolStatus struct {
	TotalMessages json.RawMessage `json:"totalMessages"`
	LocalMessages json.RawMessage `json:"localMessages"`
	PostBlock     string          `json:"postBlock"`
}

type SubmitTask struct {
	db     *harmonydb.DB
	api    SubmitTaskNodeAPI
	bstore curiochain.CurioBlockstore

	sender *message.Sender
	as     *multictladdr.MultiAddressSelector
	cfg    submitConfig
}

func NewSubmitTask(db *harmonydb.DB, api SubmitTaskNodeAPI, bstore curiochain.CurioBlockstore,
	sender *message.Sender, as *multictladdr.MultiAddressSelector, cfg *config.CurioConfig) *SubmitTask {

	return &SubmitTask{
		db:     db,
		api:    api,
		bstore: bstore,

		sender: sender,
		as:     as,

		cfg: submitConfig{
			maxFee:                     cfg.Fees.MaxCommitGasFee, // todo snap-specific
			RequireActivationSuccess:   cfg.Subsystems.RequireActivationSuccess,
			RequireNotificationSuccess: cfg.Subsystems.RequireNotificationSuccess,

			CollateralFromMinerBalance: cfg.Fees.CollateralFromMinerBalance,
			DisableCollateralFallback:  cfg.Fees.DisableCollateralFallback,
		},
	}
}

var (
	lastChecked  time.Time
	cachedResult bool
	mu           sync.Mutex
)

// Helper function to check gas fees
func checkGasFees() (bool, error) {
	mu.Lock()
	defer mu.Unlock()

	// Check if less than a minute has passed since the last API call
	if time.Since(lastChecked) < time.Minute {
		return cachedResult, nil
	}

	// Call the API if more than a minute has passed
	resp, err := http.Get("http://212.6.53.183/gasoraclelin.html")
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}

	// Check the body for "1" or "0"
	bodyStr := string(body)
	cachedResult = bodyStr == "1"
	lastChecked = time.Now()

	return cachedResult, nil
}

func (s *SubmitTask) Do(taskID harmonytask.TaskID, stillOwned func() bool) (done bool, err error) {
	lowFees, err := checkGasFees()  // Rename variable to reflect actual condition it's checking
	if err != nil {
		log.Errorw("Error checking gas fees", "error", err)
		return false, err
	}
	if !lowFees {  // Correct condition based on new variable name
		log.Infow("Gas fees are high, postponing submission.")
		return false, nil
	}
	log.Infow("Gas fees are low, proceeding with submission.")

	var tasks []struct {
		SpID         int64 `db:"sp_id"`
		SectorNumber int64 `db:"sector_number"`
		UpdateProof  int64 `db:"upgrade_proof"`

		RegSealProof int64 `db:"reg_seal_proof"`

		UpdateSealedCID   string `db:"update_sealed_cid"`
		UpdateUnsealedCID string `db:"update_unsealed_cid"`

		Proof []byte

		Deadline uint64 `db:"deadline"`
	}

	ctx := context.Background()

	err = s.db.Select(ctx, &tasks, `
		SELECT snp.sp_id, snp.sector_number, snp.upgrade_proof, sm.reg_seal_proof, snp.update_sealed_cid, snp.update_unsealed_cid, snp.proof, sm.deadline
		FROM sectors_snap_pipeline snp
		INNER JOIN sectors_meta sm ON snp.sp_id = sm.sp_id AND snp.sector_number = sm.sector_num
		WHERE snp.task_id_submit = $1`, taskID)
	if err != nil {
		return false, xerrors.Errorf("getting sector params: %w", err)
	}

	if len(tasks) != 1 {
		return false, xerrors.Errorf("expected 1 sector params, got %d", len(tasks))
	}

	update := tasks[0]

	var pieces []struct {
		Manifest json.RawMessage `db:"direct_piece_activation_manifest"`
		Size     int64           `db:"piece_size"`
		Start    int64           `db:"direct_start_epoch"`
	}
	err = s.db.Select(ctx, &pieces, `
		SELECT direct_piece_activation_manifest, piece_size, direct_start_epoch
		FROM sectors_snap_initial_pieces
		WHERE sp_id = $1 AND sector_number = $2 ORDER BY piece_index ASC`, update.SpID, update.SectorNumber)
	if err != nil {
		return false, xerrors.Errorf("getting pieces: %w", err)
	}

	ts, err := s.api.ChainHead(ctx)
	if err != nil {
		return false, xerrors.Errorf("getting chain head: %w", err)
	}

	maddr, err := address.NewIDAddress(uint64(update.SpID))
	if err != nil {
		return false, xerrors.Errorf("parsing miner address: %w", err)
	}

	snum := abi.SectorNumber(update.SectorNumber)

	onChainInfo, err := s.api.StateSectorGetInfo(ctx, maddr, snum, ts.Key())
	if err != nil {
		return false, xerrors.Errorf("getting sector info: %w", err)
	}
	if onChainInfo == nil {
		return false, xerrors.Errorf("sector not found on chain")
	}

	sl, err := s.api.StateSectorPartition(ctx, maddr, snum, types.EmptyTSK)
	if err != nil {
		return false, xerrors.Errorf("getting sector location: %w", err)
	}

	// Check that the sector isn't in an immutable deadline (or isn't about to be)
	curDl, err := s.api.StateMinerProvingDeadline(ctx, maddr, ts.Key())
	if err != nil {
		return false, xerrors.Errorf("getting current proving deadline: %w", err)
	}

	// Matches actor logic - https://github.com/filecoin-project/builtin-actors/blob/76abc47726bdbd8b478ef10e573c25957c786d1d/actors/miner/src/deadlines.rs#L65
	sectorDl := dline.NewInfo(curDl.PeriodStart, sl.Deadline, curDl.CurrentEpoch,
		curDl.WPoStPeriodDeadlines,
		curDl.WPoStProvingPeriod,
		curDl.WPoStChallengeWindow,
		curDl.WPoStChallengeLookback,
		curDl.FaultDeclarationCutoff)

	sectorDl = sectorDl.NextNotElapsed()
	firstImmutableEpoch := sectorDl.Open - curDl.WPoStChallengeWindow
	firstUnsafeEpoch := firstImmutableEpoch - ImmutableSubmitGate
	lastImmutableEpoch := sectorDl.Close

	if ts.Height() > firstUnsafeEpoch && ts.Height() < lastImmutableEpoch {
		closeTime := curiochain.EpochTime(ts, sectorDl.Close)

		log.Warnw("sector in unsafe window, delaying submit", "sp", update.SpID, "sector", update.SectorNumber, "cur_dl", curDl, "sector_dl", sectorDl, "close_time", closeTime)

		_, err := s.db.Exec(ctx, `UPDATE sectors_snap_pipeline SET
                                 task_id_submit = NULL, after_submit = FALSE, submit_after = $1
                             WHERE sp_id = $2 AND sector_number = $3`, closeTime, update.SpID, update.SectorNumber)
		if err != nil {
			return false, xerrors.Errorf("updating sector params: %w", err)
		}

		return true, nil
	}
	if ts.Height() >= lastImmutableEpoch {
		// the deadline math shouldn't allow this to ever happen, buuut just in case the math is wrong we also check the
		// upper bound of the proving window
		// (should never happen because if the current epoch is at deadline Close, NextNotElapsed will give us the next deadline)
		log.Errorw("sector in somehow past immutable window", "sp", update.SpID, "sector", update.SectorNumber, "cur_dl", curDl, "sector_dl", sectorDl)
	}

	// Process pieces, prepare PAMs
	var pams []miner.PieceActivationManifest
	var minStart abi.ChainEpoch
	var verifiedSize int64
	for _, piece := range pieces {
		var pam *miner.PieceActivationManifest
		err = json.Unmarshal(piece.Manifest, &pam)
		if err != nil {
			return false, xerrors.Errorf("marshalling json to PieceManifest: %w", err)
		}
		unrecoverable, err := seal.AllocationCheck(ctx, s.api, pam, onChainInfo.Expiration, abi.ActorID(update.SpID), ts)
		if err != nil {
			if unrecoverable {
				_, err2 := s.db.Exec(ctx, `UPDATE sectors_snap_pipeline SET 
                                 failed = TRUE, failed_at = NOW(), failed_reason = 'alloc-check', failed_reason_msg = $1,
                                 task_id_submit = NULL, after_submit = FALSE
                             WHERE sp_id = $2 AND sector_number = $3`, err.Error(), update.SpID, update.SectorNumber)

				log.Errorw("allocation check failed with an unrecoverable issue", "sp", update.SpID, "sector", update.SectorNumber, "err", err)
				return true, xerrors.Errorf("allocation check failed with an unrecoverable issue: %w", multierr.Combine(err, err2))
			}

			return false, err
		}

		if pam.VerifiedAllocationKey != nil {
			verifiedSize += piece.Size
		}

		if minStart == 0 || abi.ChainEpoch(piece.Start) < minStart {
			minStart = abi.ChainEpoch(piece.Start)
		}

		pams = append(pams, *pam)
	}

	newSealedCID, err := cid.Parse(update.UpdateSealedCID)
	if err != nil {
		return false, xerrors.Errorf("parsing new sealed cid: %w", err)
	}
	newUnsealedCID, err := cid.Parse(update.UpdateUnsealedCID)
	if err != nil {
		return false, xerrors.Errorf("parsing new unsealed cid: %w", err)
	}

	// Prepare params
	params := miner.ProveReplicaUpdates3Params{
		SectorUpdates: []miner13.SectorUpdateManifest{
			{
				Sector:       snum,
				Deadline:     sl.Deadline,
				Partition:    sl.Partition,
				NewSealedCID: newSealedCID,
				Pieces:       pams,
			},
		},
		SectorProofs:               [][]byte{update.Proof},
		AggregateProof:             nil,
		UpdateProofsType:           abi.RegisteredUpdateProof(update.UpdateProof),
		AggregateProofType:         nil,
		RequireActivationSuccess:   s.cfg.RequireActivationSuccess,
		RequireNotificationSuccess: s.cfg.RequireNotificationSuccess,
	}

	enc := new(bytes.Buffer)
	if err := params.MarshalCBOR(enc); err != nil {
		return false, xerrors.Errorf("could not serialize commit params: %w", err)
	}

	mi, err := s.api.StateMinerInfo(ctx, maddr, types.EmptyTSK)
	if err != nil {
		return false, xerrors.Errorf("getting miner info: %w", err)
	}

	ssize, err := onChainInfo.SealProof.SectorSize()
	if err != nil {
		return false, xerrors.Errorf("getting sector size: %w", err)
	}

	duration := onChainInfo.Expiration - ts.Height()

	collateral, err := s.api.StateMinerInitialPledgeForSector(ctx, duration, ssize, uint64(verifiedSize), ts.Key())
	if err != nil {
		return false, xerrors.Errorf("calculating pledge: %w", err)
	}

	collateral = big.Sub(collateral, onChainInfo.InitialPledge)
	if collateral.LessThan(big.Zero()) {
		collateral = big.Zero()
	}

	if s.cfg.CollateralFromMinerBalance {
		if s.cfg.DisableCollateralFallback {
			collateral = big.Zero()
		}
		balance, err := s.api.StateMinerAvailableBalance(ctx, maddr, types.EmptyTSK)
		if err != nil {
			if err != nil {
				return false, xerrors.Errorf("getting miner balance: %w", err)
			}
		}
		collateral = big.Sub(collateral, balance)
		if collateral.LessThan(big.Zero()) {
			collateral = big.Zero()
		}
	}

	a, _, err := s.as.AddressFor(ctx, s.api, maddr, mi, api.CommitAddr, collateral, big.Zero())
	if err != nil {
		return false, xerrors.Errorf("getting address for precommit: %w", err)
	}

	msg := &types.Message{
		To:     maddr,
		From:   a,
		Method: builtin.MethodsMiner.ProveReplicaUpdates3,
		Params: enc.Bytes(),
		Value:  collateral, // todo config for pulling from miner balance!!
	}

	mss := &api.MessageSendSpec{
		MaxFee: abi.TokenAmount(s.cfg.maxFee),
	}

	mcid, err := s.sender.Send(ctx, msg, mss, "update")
	if err != nil {
		if minStart != 0 && ts.Height() > minStart {
			_, err2 := s.db.Exec(ctx, `UPDATE sectors_snap_pipeline SET 
                                 failed = TRUE, failed_at = NOW(), failed_reason = 'start-expired', failed_reason_msg = $1,
                                 task_id_submit = NULL, after_submit = FALSE
                             WHERE sp_id = $2 AND sector_number = $3`, err.Error(), update.SpID, update.SectorNumber)

			log.Errorw("failed to push message to mpool (beyond deal start epoch)", "sp", update.SpID, "sector", update.SectorNumber, "err", err)

			return true, xerrors.Errorf("pushing message to mpool (beyond deal start epoch): %w", multierr.Combine(err, err2))
		}

		return false, xerrors.Errorf("pushing message to mpool (minStart %d, timeTo %d): %w", minStart, minStart-ts.Height(), err)
	}

	_, err = s.db.Exec(ctx, `UPDATE sectors_snap_pipeline SET prove_msg_cid = $1, task_id_submit = NULL, after_submit = TRUE WHERE task_id_submit = $2`, mcid.String(), taskID)
	if err != nil {
		return false, xerrors.Errorf("updating sector params: %w", err)
	}

	_, err = s.db.Exec(ctx, `INSERT INTO message_waits (signed_message_cid) VALUES ($1)`, mcid)
	if err != nil {
		return false, xerrors.Errorf("inserting into message_waits: %w", err)
	}

	if err := s.transferUpdatedSectorData(ctx, update.SpID, update.SectorNumber, newUnsealedCID, newSealedCID, mcid); err != nil {
		return false, xerrors.Errorf("updating sector meta: %w", err)
	}

	return true, nil
}

func (s *SubmitTask) transferUpdatedSectorData(ctx context.Context, spID, sectorNum int64, newUns, newSl, mcid cid.Cid) error {
	if _, err := s.db.Exec(ctx, `UPDATE sectors_meta SET cur_sealed_cid = $1,
	                        		cur_unsealed_cid = $2, msg_cid_update = $3
	                        		WHERE sp_id = $4 AND sector_num = $5`, newSl.String(), newUns.String(), mcid.String(), spID, sectorNum); err != nil {
		return xerrors.Errorf("updating sector meta: %w", err)
	}

	// Execute the query for piece metadata
	if _, err := s.db.Exec(ctx, `
        INSERT INTO sectors_meta_pieces (
            sp_id,
            sector_num,
            piece_num,
            piece_cid,
            piece_size,
            requested_keep_data,
            raw_data_size,
            start_epoch,
            orig_end_epoch,
            f05_deal_id,
            ddo_pam,
            f05_deal_proposal                          
        )
        SELECT
            sp_id,
            sector_number AS sector_num,
            piece_index AS piece_num,
            piece_cid,
            piece_size,
            not data_delete_on_finalize as requested_keep_data,
            data_raw_size,
            direct_start_epoch as start_epoch,
            direct_end_epoch as orig_end_epoch,
            NULL,
            direct_piece_activation_manifest as ddo_pam,
            NULL
        FROM
            sectors_snap_initial_pieces
        WHERE
            sp_id = $1 AND
            sector_number = $2
        ON CONFLICT (sp_id, sector_num, piece_num) DO UPDATE SET
            piece_cid = excluded.piece_cid,
            piece_size = excluded.piece_size,
            requested_keep_data = excluded.requested_keep_data,
            raw_data_size = excluded.raw_data_size,
            start_epoch = excluded.start_epoch,
            orig_end_epoch = excluded.orig_end_epoch,
            f05_deal_id = excluded.f05_deal_id,
            ddo_pam = excluded.ddo_pam,
            f05_deal_proposal = excluded.f05_deal_proposal;
    `, spID, sectorNum); err != nil {
		return fmt.Errorf("failed to insert/update sector_meta_pieces: %w", err)
	}

	return nil
}

func (s *SubmitTask) CanAccept(ids []harmonytask.TaskID, engine *harmonytask.TaskEngine) (*harmonytask.TaskID, error) {
	id := ids[0]
	return &id, nil
}

func (s *SubmitTask) TypeDetails() harmonytask.TaskTypeDetails {
	return harmonytask.TaskTypeDetails{
		Name: "UpdateSubmit",
		Cost: resources.Resources{
			Cpu: 1,
			Ram: 64 << 20,
		},
		MaxFailures: 3,
		IAmBored: passcall.Every(MinSnapSchedInterval, func(taskFunc harmonytask.AddTaskFunc) error {
			return s.schedule(context.Background(), taskFunc)
		}),
	}
}

// Check the mpool to avoid schedule submits when we are already loaded.
var (
	lastMpoolChecked  time.Time
	cachedMpoolResult bool
	mpoolMutex        sync.Mutex
)


func parseRawMessageAsIntSum(raw json.RawMessage) (int, error) {
	var intValue int
	if err := json.Unmarshal(raw, &intValue); err == nil {
		return intValue, nil
	}

	var stringValue string
	if err := json.Unmarshal(raw, &stringValue); err == nil {
		return strconv.Atoi(stringValue)
	}

	var intArray []int
	if err := json.Unmarshal(raw, &intArray); err == nil {
		sum := 0
		for _, v := range intArray {
			sum += v
		}
		return sum, nil
	}

	var stringArray []string
	if err := json.Unmarshal(raw, &stringArray); err == nil {
		sum := 0
		for _, v := range stringArray {
			intVal, err := strconv.Atoi(v)
			if err != nil {
				return 0, fmt.Errorf("invalid integer in string array: %w", err)
			}
			sum += intVal
		}
		return sum, nil
	}

	return 0, fmt.Errorf("unsupported type for field: %s", string(raw))
}


func checkMpoolStatus(actor string) (bool, error) {
	mpoolMutex.Lock()
	defer mpoolMutex.Unlock()

	// Check if cached result is still valid
	if time.Since(lastMpoolChecked) < 30*time.Second {
		return cachedMpoolResult, nil
	}

	// Make the API call
	url := fmt.Sprintf("http://212.6.53.183/mpool.php?actor=%s", actor)
	resp, err := http.Get(url)
	if err != nil {
		return false, fmt.Errorf("error calling mpool API: %w", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("error reading mpool API response: %w", err)
	}

	var status MpoolStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return false, fmt.Errorf("error unmarshalling mpool API response: %w", err)
	}

	// Parse and validate fields
	totalMessages, err := parseRawMessageAsIntSum(status.TotalMessages)
	if err != nil {
		return false, fmt.Errorf("error parsing totalMessages: %w", err)
	}

	localMessages, err := parseRawMessageAsIntSum(status.LocalMessages)
	if err != nil {
		return false, fmt.Errorf("error parsing localMessages: %w", err)
	}

	// Check constraints
	if totalMessages > 10000 || localMessages > 20 || status.PostBlock == "yes" {
		cachedMpoolResult = false
	} else {
		cachedMpoolResult = true
	}

	// Update the last checked time
	lastMpoolChecked = time.Now()

	return cachedMpoolResult, nil
}

// Helper to parse json.RawMessage as an integer
func parseRawMessageAsInt(raw json.RawMessage) (int, error) {
    var result int
    if err := json.Unmarshal(raw, &result); err == nil {
        return result, nil
    }

    var str string
    if err := json.Unmarshal(raw, &str); err == nil {
        return strconv.Atoi(str)
    }

    return 0, fmt.Errorf("unsupported type for integer field: %s", string(raw))
}

// Helper to parse json.RawMessage as an array of integers and return their sum
func parseRawMessageAsIntArraySum(raw json.RawMessage) (int, error) {
    var array []int
    if err := json.Unmarshal(raw, &array); err == nil {
        sum := 0
        for _, v := range array {
            sum += v
        }
        return sum, nil
    }

    var singleValue int
    if err := json.Unmarshal(raw, &singleValue); err == nil {
        return singleValue, nil
    }

    var str string
    if err := json.Unmarshal(raw, &str); err == nil {
        return strconv.Atoi(str)
    }

    return 0, fmt.Errorf("unsupported type for integer array field: %s", string(raw))
}

var (
	lastScheduled time.Time
	scheduleMutex sync.Mutex
)

func shouldSchedule() bool {
	scheduleMutex.Lock()
	defer scheduleMutex.Unlock()

	// Check if enough time has passed since the last scheduling
	if time.Since(lastScheduled) < time.Minute {
		return false
	}

	// Update the last scheduled time
	lastScheduled = time.Now()
	return true
}

func (s *SubmitTask) schedule(ctx context.Context, taskFunc harmonytask.AddTaskFunc) error {
    if !shouldSchedule() {
        log.Infow("Skipping schedule to maintain 1-minute rate limit.")
        return nil
    }

    // Check gas fees before scheduling
    lowFees, err := checkGasFees()
    if err != nil {
        log.Errorw("Error checking gas fees", "error", err)
        return err
    }
    if !lowFees {
        log.Infow("Gas fees are high, postponing scheduling.")
        return nil
    }

    log.Infow("Gas fees are low, proceeding with scheduling.")

    // Schedule the task
    var stop bool
    for !stop {
        taskFunc(func(id harmonytask.TaskID, tx *harmonydb.Tx) (shouldCommit bool, seriousError error) {
            stop = true // Assume we're done until we find a task to schedule

            var tasks []struct {
                SpID         int64 `db:"sp_id"`
                SectorNumber int64 `db:"sector_number"`
            }

            // Select eligible tasks from the database
            err := tx.Select(&tasks, `SELECT sp_id, sector_number FROM sectors_snap_pipeline WHERE failed = FALSE
                AND after_encode = TRUE
                AND after_prove = TRUE
                AND after_submit = FALSE
                AND (submit_after IS NULL OR submit_after < NOW())
                AND task_id_submit IS NULL`)
            if err != nil {
                return false, xerrors.Errorf("getting tasks: %w", err)
            }

            if len(tasks) == 0 {
                return false, nil
            }

            // Pick a random task
            t := tasks[rand.N(len(tasks))]

            // Use SpID for actor in the mpool check
            actor := fmt.Sprintf("%d", t.SpID)
            mpoolAllowed, err := checkMpoolStatus(actor)
            if err != nil {
                log.Errorw("Error checking mpool status", "error", err)
                return false, err
            }
            if !mpoolAllowed {
                log.Infow("Mpool constraints not met, postponing scheduling.")
                return false, nil
            }

            // Update the database with the scheduled task
            _, err = tx.Exec(`UPDATE sectors_snap_pipeline SET task_id_submit = $1, submit_after = NULL WHERE sp_id = $2 AND sector_number = $3`, id, t.SpID, t.SectorNumber)
            if err != nil {
                return false, xerrors.Errorf("updating task id: %w", err)
            }

            log.Infow("Task scheduled successfully", "spID", t.SpID, "sectorNumber", t.SectorNumber)
            return true, nil
        })
    }

    // Update landed tasks
    var tasks []struct {
        SpID         int64 `db:"sp_id"`
        SectorNumber int64 `db:"sector_number"`
    }

    err = s.db.Select(ctx, &tasks, `SELECT sp_id, sector_number FROM sectors_snap_pipeline WHERE after_encode = TRUE AND after_prove = TRUE AND after_prove_msg_success = FALSE AND after_submit = TRUE`)
    if err != nil {
        return xerrors.Errorf("getting tasks: %w", err)
    }

    for _, t := range tasks {
        if err := s.updateLanded(ctx, t.SpID, t.SectorNumber); err != nil {
            log.Errorw("Updating landed", "sp", t.SpID, "sector", t.SectorNumber, "err", err)
        }
    }

    return nil
}

func (s *SubmitTask) updateLanded(ctx context.Context, spId, sectorNum int64) error {
	var execResult []struct {
		ProveMsgCID          string `db:"prove_msg_cid"`
		UpdateSealedCID      string `db:"update_sealed_cid"`
		ExecutedTskCID       string `db:"executed_tsk_cid"`
		ExecutedTskEpoch     int64  `db:"executed_tsk_epoch"`
		ExecutedMsgCID       string `db:"executed_msg_cid"`
		ExecutedRcptExitCode int64  `db:"executed_rcpt_exitcode"`
		ExecutedRcptGasUsed  int64  `db:"executed_rcpt_gas_used"`
	}

	err := s.db.Select(ctx, &execResult, `SELECT spipeline.prove_msg_cid, spipeline.update_sealed_cid, executed_tsk_cid, executed_tsk_epoch, executed_msg_cid, executed_rcpt_exitcode, executed_rcpt_gas_used
					FROM sectors_snap_pipeline spipeline
					JOIN message_waits ON spipeline.prove_msg_cid = message_waits.signed_message_cid
					WHERE sp_id = $1 AND sector_number = $2 AND executed_tsk_epoch IS NOT NULL`, spId, sectorNum)
	if err != nil {
		return xerrors.Errorf("failed to query message_waits: %w", err)
	}

	if len(execResult) > 0 {
		maddr, err := address.NewIDAddress(uint64(spId))
		if err != nil {
			return err
		}
		switch exitcode.ExitCode(execResult[0].ExecutedRcptExitCode) {
		case exitcode.Ok:
			// good, noop
		case exitcode.SysErrInsufficientFunds, exitcode.ErrInsufficientFunds:
			fallthrough
		case exitcode.SysErrOutOfGas:
			// just retry
			n, err := s.db.Exec(ctx, `UPDATE sectors_snap_pipeline SET
						after_prove_msg_success = FALSE, after_submit = FALSE
						WHERE sp_id = $2 AND sector_number = $3 AND after_prove_msg_success = FALSE AND after_submit = TRUE`,
				execResult[0].ExecutedTskCID, spId, sectorNum)
			if err != nil {
				return xerrors.Errorf("update sectors_snap_pipeline to retry prove send: %w", err)
			}
			if n == 0 {
				return xerrors.Errorf("update sectors_snap_pipeline to retry prove send: no rows updated")
			}
			return nil
		case exitcode.ErrNotFound:
			// message not found, but maybe it's fine?

			si, err := s.api.StateSectorGetInfo(ctx, maddr, abi.SectorNumber(sectorNum), types.EmptyTSK)
			if err != nil {
				return xerrors.Errorf("get sector info: %w", err)
			}
			if si != nil && si.SealedCID.String() == execResult[0].UpdateSealedCID {
				return nil
			}

			return xerrors.Errorf("sector info after prove message not found not as expected")
		default:
			return xerrors.Errorf("commit message failed with exit code %s", exitcode.ExitCode(execResult[0].ExecutedRcptExitCode))
		}

		si, err := s.api.StateSectorGetInfo(ctx, maddr, abi.SectorNumber(sectorNum), types.EmptyTSK)
		if err != nil {
			return xerrors.Errorf("get sector info: %w", err)
		}

		if si == nil {
			log.Errorw("todo handle missing sector info (not found after cron)", "sp", spId, "sector", sectorNum, "exec_epoch", execResult[0].ExecutedTskEpoch, "exec_tskcid", execResult[0].ExecutedTskCID, "msg_cid", execResult[0].ExecutedMsgCID)
			// todo handdle missing sector info (not found after cron)
		} else {
			if si.SealedCID.String() != execResult[0].UpdateSealedCID {
				log.Errorw("sector sealed CID mismatch after update?!", "sp", spId, "sector", sectorNum, "exec_epoch", execResult[0].ExecutedTskEpoch, "exec_tskcid", execResult[0].ExecutedTskCID, "msg_cid", execResult[0].ExecutedMsgCID)
				return nil
			}
			// yay!

			_, err := s.db.Exec(ctx, `UPDATE sectors_snap_pipeline SET
						after_prove_msg_success = TRUE, prove_msg_tsk = $1
						WHERE sp_id = $2 AND sector_number = $3 AND after_prove_msg_success = FALSE`,
				execResult[0].ExecutedTskCID, spId, sectorNum)
			if err != nil {
				return xerrors.Errorf("update sectors_snap_pipeline: %w", err)
			}
		}
	}

	return nil
}

func (s *SubmitTask) Adder(taskFunc harmonytask.AddTaskFunc) {
}

func (s *SubmitTask) GetSpid(db *harmonydb.DB, taskID int64) string {
	sid, err := s.GetSectorID(db, taskID)
	if err != nil {
		log.Errorf("getting sector id: %s", err)
		return ""
	}
	return sid.Miner.String()
}

func (s *SubmitTask) GetSectorID(db *harmonydb.DB, taskID int64) (*abi.SectorID, error) {
	var spId, sectorNumber uint64
	err := db.QueryRow(context.Background(), `SELECT sp_id,sector_number FROM sectors_snap_pipeline WHERE task_id_submit = $1`, taskID).Scan(&spId, &sectorNumber)
	if err != nil {
		return nil, err
	}
	return &abi.SectorID{
		Miner:  abi.ActorID(spId),
		Number: abi.SectorNumber(sectorNumber),
	}, nil
}

var _ = harmonytask.Reg(&SubmitTask{})
var _ harmonytask.TaskInterface = &SubmitTask{}
