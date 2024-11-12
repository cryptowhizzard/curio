package seal

import (
	"context"
	"time"

	"golang.org/x/xerrors"
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/curio/harmony/harmonydb"
	"github.com/filecoin-project/curio/harmony/harmonytask"
	"github.com/filecoin-project/curio/harmony/resources"
	"github.com/filecoin-project/curio/harmony/taskhelp"
	"github.com/filecoin-project/curio/lib/ffi"
	"github.com/filecoin-project/curio/lib/slotmgr"
	storiface "github.com/filecoin-project/curio/lib/storiface"
)

type FinalizeTask struct {
	max int
	sp  *SealPoller
	sc  *ffi.SealCalls
	db  *harmonydb.DB

	// Batch, nillable!
	slots *slotmgr.SlotMgr
}

func NewFinalizeTask(max int, sp *SealPoller, sc *ffi.SealCalls, db *harmonydb.DB, slots *slotmgr.SlotMgr) *FinalizeTask {
	return &FinalizeTask{
		max: max,
		sp:  sp,
		sc:  sc,
		db:  db,

		slots: slots,
	}
}

func (f *FinalizeTask) GetSpid(db *harmonydb.DB, taskID int64) string {
	sid, err := f.GetSectorID(db, taskID)
	if err != nil {
		log.Errorf("getting sector id: %s", err)
		return ""
	}
	return sid.Miner.String()
}

func (f *FinalizeTask) GetSectorID(db *harmonydb.DB, taskID int64) (*abi.SectorID, error) {
	var spId, sectorNumber uint64
	err := db.QueryRow(context.Background(), `SELECT sp_id,sector_number FROM sectors_sdr_pipeline WHERE task_id_finalize = $1`, taskID).Scan(&spId, &sectorNumber)
	if err != nil {
		return nil, err
	}
	return &abi.SectorID{
		Miner:  abi.ActorID(spId),
		Number: abi.SectorNumber(sectorNumber),
	}, nil
}

var _ = harmonytask.Reg(&FinalizeTask{})

func (f *FinalizeTask) Do(taskID harmonytask.TaskID, stillOwned func() bool) (done bool, err error) {
    var tasks []struct {
        SpID         int64 `db:"sp_id"`
        SectorNumber int64 `db:"sector_number"`
        RegSealProof int64 `db:"reg_seal_proof"`
    }

    ctx := context.Background()

    err = f.db.Select(ctx, &tasks, `
        SELECT sp_id, sector_number, reg_seal_proof FROM sectors_sdr_pipeline WHERE task_id_finalize = $1`, taskID)
    if err != nil {
        return false, xerrors.Errorf("getting task: %w", err)
    }

    if len(tasks) != 1 {
        return false, xerrors.Errorf("expected one task")
    }
    task := tasks[0]

    var keepUnsealed bool

    if err := f.db.QueryRow(ctx, `SELECT COALESCE(BOOL_OR(NOT data_delete_on_finalize), FALSE) FROM sectors_sdr_initial_pieces WHERE sp_id = $1 AND sector_number = $2`, task.SpID, task.SectorNumber).Scan(&keepUnsealed); err != nil {
        return false, err
    }

    sector := storiface.SectorRef{
        ID: abi.SectorID{
            Miner:  abi.ActorID(task.SpID),
            Number: abi.SectorNumber(task.SectorNumber),
        },
        ProofType: abi.RegisteredSealProof(task.RegSealProof),
    }

    var ownedBy []struct {
        HostAndPort string `db:"host_and_port"`
    }
    var refs []struct {
        PipelineSlot int64 `db:"pipeline_slot"`
    }

    if f.slots != nil {
        // batch handling part 1:
        // get machine id

        err = f.db.Select(ctx, &ownedBy, `SELECT hm.host_and_port as host_and_port FROM harmony_task INNER JOIN harmony_machines hm on harmony_task.owner_id = hm.id WHERE harmony_task.id = $1`, taskID)
        if err != nil {
            return false, xerrors.Errorf("getting machine id: %w", err)
        }

        if len(ownedBy) != 1 {
            return false, xerrors.Errorf("expected one machine")
        }

        err = f.db.Select(ctx, &refs, `SELECT pipeline_slot FROM batch_sector_refs WHERE sp_id = $1 AND sector_number = $2 AND machine_host_and_port = $3`, task.SpID, task.SectorNumber, ownedBy[0].HostAndPort)
        if err != nil {
            return false, xerrors.Errorf("getting batch refs: %w", err)
        }

        // **Loop through all refs (slots) for multiple slot handling**
        for _, ref := range refs {
            go f.startPeriodicBatchCheck(ctx, ownedBy[0].HostAndPort, ref.PipelineSlot)
        }
    }

    // Finalize the sector and update database as needed (no changes to this part)
    err = f.sc.FinalizeSector(ctx, sector, keepUnsealed)
    if err != nil {
        return false, xerrors.Errorf("finalizing sector: %w", err)
    }

    if err := DropSectorPieceRefs(ctx, f.db, sector.ID); err != nil {
        return false, xerrors.Errorf("dropping sector piece refs: %w", err)
    }

    // Set after_finalize
    _, err = f.db.Exec(ctx, `UPDATE sectors_sdr_pipeline SET after_finalize = TRUE, task_id_finalize = NULL WHERE task_id_finalize = $1`, taskID)
    if err != nil {
        return false, xerrors.Errorf("updating task: %w", err)
    }

    return true, nil
}

// New function for periodic batch check with additional debugging logs
func (f *FinalizeTask) startPeriodicBatchCheck(ctx context.Context, hostAndPort string, pipelineSlot int64) {
	log.Infow("Starting periodic batch check", "host", hostAndPort, "slot", pipelineSlot)
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Infow("Context canceled, stopping periodic batch check", "host", hostAndPort, "slot", pipelineSlot)
			return
		case <-ticker.C:
			log.Infow("Running periodic check and logging pipeline status", "host", hostAndPort, "slot", pipelineSlot)
			f.checkAndLogPipelineStatus(ctx, hostAndPort, pipelineSlot)
			f.checkAndFreeBatchSlot(ctx, hostAndPort, pipelineSlot)
		}
	}
}

// New function to log the current pipeline status with additional debugging
func (f *FinalizeTask) checkAndLogPipelineStatus(ctx context.Context, hostAndPort string, pipelineSlot int64) {
	log.Infow("Checking pipeline status", "host", hostAndPort, "slot", pipelineSlot)
	var count int64
	err := f.db.QueryRow(ctx, `SELECT COUNT(1) as count FROM batch_sector_refs WHERE machine_host_and_port = $1 AND pipeline_slot = $2`, hostAndPort, pipelineSlot).Scan(&count)
	if err != nil {
		log.Errorw("Error checking batch ref count for logging", "error", err)
		return
	}

	// Log the current number of sectors in the pipeline and the host they are open on
	log.Infow("Pipeline status check", "remaining_sectors", count, "host", hostAndPort, "slot", pipelineSlot)
}

// Existing function to check and free batch slot if no sectors are pending
func (f *FinalizeTask) checkAndFreeBatchSlot(ctx context.Context, hostAndPort string, pipelineSlot int64) {
	var count int64
	err := f.db.QueryRow(ctx, `SELECT COUNT(1) as count FROM batch_sector_refs WHERE machine_host_and_port = $1 AND pipeline_slot = $2`, hostAndPort, pipelineSlot).Scan(&count)
	if err != nil {
		log.Errorw("Error checking batch ref count", "error", err)
		return
	}

	if count == 0 {
		log.Infow("No sectors remaining, freeing batch slot", "slot", pipelineSlot, "machine", hostAndPort)
		if err := f.slots.Put(uint64(pipelineSlot)); err != nil {
			log.Errorw("Error freeing slot", "slot", pipelineSlot, "error", err)
		} else {
			log.Infow("Successfully freed batch slot", "slot", pipelineSlot)
		}
	} else {
		log.Infow("Sectors still remaining in the batch", "remaining", count)
	}
}

func (f *FinalizeTask) CanAccept(ids []harmonytask.TaskID, engine *harmonytask.TaskEngine) (*harmonytask.TaskID, error) {
	var tasks []struct {
		TaskID       harmonytask.TaskID `db:"task_id_finalize"`
		SpID         int64              `db:"sp_id"`
		SectorNumber int64              `db:"sector_number"`
		StorageID    string             `db:"storage_id"`
	}

	if storiface.FTCache != 4 {
		panic("storiface.FTCache != 4")
	}

	ctx := context.Background()

	indIDs := make([]int64, len(ids))
	for i, id := range ids {
		indIDs[i] = int64(id)
	}

	err := f.db.Select(ctx, &tasks, `
		SELECT p.task_id_finalize, p.sp_id, p.sector_number, l.storage_id FROM sectors_sdr_pipeline p
			INNER JOIN sector_location l ON p.sp_id = l.miner_id AND p.sector_number = l.sector_num
			WHERE task_id_finalize = ANY ($1) AND l.sector_filetype = 4
`, indIDs)
	if err != nil {
		return nil, xerrors.Errorf("getting tasks: %w", err)
	}

	ls, err := f.sc.LocalStorage(ctx)
	if err != nil {
		return nil, xerrors.Errorf("getting local storage: %w", err)
	}

	acceptables := map[harmonytask.TaskID]bool{}

	for _, t := range ids {
		acceptables[t] = true
	}

	for _, t := range tasks {
		if _, ok := acceptables[t.TaskID]; !ok {
			continue
		}

		for _, l := range ls {
			if string(l.ID) == t.StorageID {
				return &t.TaskID, nil
			}
		}
	}

	return nil, nil
}

func (f *FinalizeTask) TypeDetails() harmonytask.TaskTypeDetails {
	return harmonytask.TaskTypeDetails{
		Max:  taskhelp.Max(f.max),
		Name: "Finalize",
		Cost: resources.Resources{
			Cpu: 1,
			Gpu: 0,
			Ram: 100 << 20,
		},
		MaxFailures: 10,
	}
}

func (f *FinalizeTask) Adder(taskFunc harmonytask.AddTaskFunc) {
	f.sp.pollers[pollerFinalize].Set(taskFunc)
}

var _ harmonytask.TaskInterface = &FinalizeTask{}
