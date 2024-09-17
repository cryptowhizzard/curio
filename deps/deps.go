// Package deps provides the dependencies for the curio node.
package deps

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/gbrlsnchs/jwt/v3"
	logging "github.com/ipfs/go-log/v2"
	"github.com/samber/lo"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/curio/alertmanager"
	"github.com/filecoin-project/curio/alertmanager/curioalerting"
	"github.com/filecoin-project/curio/api"
	"github.com/filecoin-project/curio/deps/config"
	"github.com/filecoin-project/curio/harmony/harmonydb"
	"github.com/filecoin-project/curio/lib/curiochain"
	"github.com/filecoin-project/curio/lib/multictladdr"
	"github.com/filecoin-project/curio/lib/paths"
	"github.com/filecoin-project/curio/lib/pieceprovider"
	"github.com/filecoin-project/curio/lib/repo"
	"github.com/filecoin-project/curio/lib/storiface"
	"github.com/filecoin-project/curio/market/indexstore"

	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
	lrepo "github.com/filecoin-project/lotus/node/repo"
	"github.com/filecoin-project/lotus/storage/sealer"
	"github.com/filecoin-project/lotus/storage/sealer/ffiwrapper"
)

var log = logging.Logger("curio/deps")

func MakeDB(cctx *cli.Context) (*harmonydb.DB, error) {
	// #1 CLI opts
	fromCLI := func() (*harmonydb.DB, error) {
		dbConfig := config.HarmonyDB{
			Username: cctx.String("db-user"),
			Password: cctx.String("db-password"),
			Hosts:    strings.Split(cctx.String("db-host"), ","),
			Database: cctx.String("db-name"),
			Port:     cctx.String("db-port"),
		}
		return harmonydb.NewFromConfig(dbConfig)
	}

	readToml := func(path string) (*harmonydb.DB, error) {
		cfg, err := config.FromFile(path)
		if err != nil {
			return nil, err
		}
		if c, ok := cfg.(*config.StorageMiner); ok {
			return harmonydb.NewFromConfig(c.HarmonyDB)
		}
		return nil, errors.New("not a miner config")
	}

	// #2 Try local miner config
	fromMinerEnv := func() (*harmonydb.DB, error) {
		v := os.Getenv("LOTUS_MINER_PATH")
		if v == "" {
			return nil, errors.New("no miner env")
		}
		return readToml(filepath.Join(v, "config.toml"))

	}

	fromMiner := func() (*harmonydb.DB, error) {
		u, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		return readToml(filepath.Join(u, ".lotusminer/config.toml"))
	}
	fromEnv := func() (*harmonydb.DB, error) {
		// #3 Try env
		u, err := url.Parse(os.Getenv("CURIO_DB"))
		if err != nil {
			return nil, errors.New("no db connection string found in CURIO_DB env")
		}
		cfg := config.DefaultStorageMiner().HarmonyDB
		if u.User.Username() != "" {
			cfg.Username = u.User.Username()
		}
		if p, ok := u.User.Password(); ok && p != "" {
			cfg.Password = p
		}
		if u.Hostname() != "" {
			cfg.Hosts = []string{u.Hostname()}
		}
		if u.Port() != "" {
			cfg.Port = u.Port()
		}
		if strings.TrimPrefix(u.Path, "/") != "" {
			cfg.Database = strings.TrimPrefix(u.Path, "/")
		}

		return harmonydb.NewFromConfig(cfg)
	}

	for _, f := range []func() (*harmonydb.DB, error){fromCLI, fromMinerEnv, fromMiner, fromEnv} {
		db, err := f()
		if err != nil {
			continue
		}
		return db, nil
	}
	log.Error("No db connection string found. User CLI args or env var: set CURIO_DB=postgres://USER:PASSWORD@HOST:PORT/DATABASE")
	return fromCLI() //in-case it's not about bad config.
}

type JwtPayload struct {
	Allow []auth.Permission
}

func StorageAuth(apiKey string) (sealer.StorageAuth, error) {
	if apiKey == "" {
		return nil, xerrors.Errorf("no api key provided")
	}

	rawKey, err := base64.StdEncoding.DecodeString(apiKey)
	if err != nil {
		return nil, xerrors.Errorf("decoding api key: %w", err)
	}

	key := jwt.NewHS256(rawKey)

	p := JwtPayload{
		Allow: []auth.Permission{"admin"},
	}

	token, err := jwt.Sign(&p, key)
	if err != nil {
		return nil, err
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(token))
	return sealer.StorageAuth(headers), nil
}

func GetDeps(ctx context.Context, cctx *cli.Context) (*Deps, error) {
	var deps Deps
	return &deps, deps.PopulateRemainingDeps(ctx, cctx, true)
}

type Deps struct {
	Layers        []string
	Cfg           *config.CurioConfig // values
	DB            *harmonydb.DB       // has itest capability
	Chain         api.Chain
	Bstore        curiochain.CurioBlockstore
	Verif         storiface.Verifier
	As            *multictladdr.MultiAddressSelector
	Maddrs        map[dtypes.MinerAddress]bool
	ProofTypes    map[abi.RegisteredSealProof]bool
	Stor          *paths.Remote
	Al            *curioalerting.AlertingSystem
	Si            paths.SectorIndex
	LocalStore    *paths.Local
	LocalPaths    *paths.BasicLocalStorage
	ListenAddr    string
	Name          string
	Alert         *alertmanager.AlertNow
	IndexStore    *indexstore.IndexStore
	PieceProvider *pieceprovider.PieceProvider
}

const (
	FlagRepoPath = "repo-path"
)

func (deps *Deps) PopulateRemainingDeps(ctx context.Context, cctx *cli.Context, makeRepo bool) error {
	var err error
	if makeRepo {
		// Open repo
		repoPath := cctx.String(FlagRepoPath)
		fmt.Println("repopath", repoPath)
		r, err := lrepo.NewFS(repoPath)
		if err != nil {
			return err
		}

		ok, err := r.Exists()
		if err != nil {
			return err
		}
		if !ok {
			if err := r.Init(repo.Curio); err != nil {
				return err
			}
		}
	}

	if deps.DB == nil {
		deps.DB, err = MakeDB(cctx)
		if err != nil {
			return err
		}
	}
	if deps.Layers == nil {
		deps.Layers = append([]string{"base"}, cctx.StringSlice("layers")...) // Always stack on top of "base" layer
	}

	if deps.Cfg == nil {
		// The config feeds into task runners & their helpers
		deps.Cfg, err = GetConfig(cctx.Context, cctx.StringSlice("layers"), deps.DB)
		if err != nil {
			return xerrors.Errorf("populate config: %w", err)
		}
	}

	log.Debugw("config", "config", deps.Cfg)

	if deps.Verif == nil {
		deps.Verif = ffiwrapper.ProofVerifier
	}

	if deps.As == nil {
		deps.As, err = multictladdr.AddressSelector(deps.Cfg.Addresses)()
		if err != nil {
			return err
		}
	}

	if deps.Al == nil {
		deps.Al = curioalerting.NewAlertingSystem()
	}

	if deps.Si == nil {
		deps.Si = paths.NewDBIndex(deps.Al, deps.DB)
	}

	if deps.Chain == nil {
		var fullCloser func()
		cfgApiInfo := deps.Cfg.Apis.ChainApiInfo
		if v := os.Getenv("FULLNODE_API_INFO"); v != "" {
			cfgApiInfo = []string{v}
		}
		deps.Chain, fullCloser, err = GetFullNodeAPIV1Curio(cctx, cfgApiInfo)
		if err != nil {
			return err
		}

		go func() {
			<-ctx.Done()
			fullCloser()
		}()
	}

	if deps.Bstore == nil {
		deps.Bstore = curiochain.NewChainBlockstore(deps.Chain)
	}

	deps.LocalPaths = &paths.BasicLocalStorage{
		PathToJSON: path.Join(cctx.String(FlagRepoPath), "storage.json"),
	}

	if deps.ListenAddr == "" {
		listenAddr := cctx.String("listen")
		const unspecifiedAddress = "0.0.0.0"
		addressSlice := strings.Split(listenAddr, ":")
		if ip := net.ParseIP(addressSlice[0]); ip != nil {
			if ip.String() == unspecifiedAddress {
				rip, err := deps.DB.GetRoutableIP()
				if err != nil {
					return err
				}
				deps.ListenAddr = rip + ":" + addressSlice[1]
			} else {
				deps.ListenAddr = ip.String() + ":" + addressSlice[1]
			}
		}
	}

	if deps.Alert == nil {
		deps.Alert = alertmanager.NewAlertNow(deps.DB, deps.ListenAddr)
	}

	if cctx.IsSet("gui-listen") {
		deps.Cfg.Subsystems.GuiAddress = cctx.String("gui-listen")
	}
	if deps.LocalStore == nil {
		deps.LocalStore, err = paths.NewLocal(ctx, deps.LocalPaths, deps.Si, []string{"http://" + deps.ListenAddr + "/remote"})
		if err != nil {
			return err
		}
	}

	sa, err := StorageAuth(deps.Cfg.Apis.StorageRPCSecret)
	if err != nil {
		return xerrors.Errorf(`'%w' while parsing the config toml's 
	[Apis]
	StorageRPCSecret=%v
Get it with: jq .PrivateKey ~/.lotus-miner/keystore/MF2XI2BNNJ3XILLQOJUXMYLUMU`, err, deps.Cfg.Apis.StorageRPCSecret)
	}
	if deps.Stor == nil {
		deps.Stor = paths.NewRemote(deps.LocalStore, deps.Si, http.Header(sa), 10, &paths.DefaultPartialFileHandler{})
	}

	if deps.Maddrs == nil {
		deps.Maddrs = map[dtypes.MinerAddress]bool{}
	}
	if len(deps.Maddrs) == 0 {
		for _, s := range deps.Cfg.Addresses {
			for _, s := range s.MinerAddresses {
				addr, err := address.NewFromString(s)
				if err != nil {
					return err
				}
				deps.Maddrs[dtypes.MinerAddress(addr)] = true
			}
		}
	}

	if deps.ProofTypes == nil {
		deps.ProofTypes = map[abi.RegisteredSealProof]bool{}
	}
	if len(deps.ProofTypes) == 0 {
		for maddr := range deps.Maddrs {
			spt, err := sealProofType(maddr, deps.Chain)
			if err != nil {
				return err
			}
			deps.ProofTypes[spt] = true
		}
	}

	if deps.Name == "" {
		deps.Name = cctx.String("name")
	}

	if deps.IndexStore == nil {
		deps.IndexStore, err = indexstore.NewIndexStore(strings.Split(cctx.String("db-host"), ","), deps.Cfg)
		if err != nil {
			return xerrors.Errorf("failed to start index store: %w", err)
		}
	}

	if deps.PieceProvider == nil {
		deps.PieceProvider = pieceprovider.NewPieceProvider(deps.Stor, deps.Si)
	}

	return nil
}

func sealProofType(maddr dtypes.MinerAddress, fnapi api.Chain) (abi.RegisteredSealProof, error) {
	mi, err := fnapi.StateMinerInfo(context.TODO(), address.Address(maddr), types.EmptyTSK)
	if err != nil {
		return 0, err
	}
	networkVersion, err := fnapi.StateNetworkVersion(context.TODO(), types.EmptyTSK)
	if err != nil {
		return 0, err
	}

	// node seal proof type does not decide whether or not we use synthetic porep
	return miner.PreferredSealProofTypeFromWindowPoStType(networkVersion, mi.WindowPoStProofType, false)
}

func LoadConfigWithUpgrades(text string, curioConfigWithDefaults *config.CurioConfig) (toml.MetaData, error) {
	// allow migration from old config format that was limited to 1 wallet setup.
	newText := strings.Join(lo.Map(strings.Split(text, "\n"), func(line string, _ int) string {
		if strings.EqualFold(line, "[addresses]") {
			return "[[addresses]]"
		}
		return line
	}), "\n")
	meta, err := toml.Decode(newText, &curioConfigWithDefaults)
	for i := range curioConfigWithDefaults.Addresses {
		if curioConfigWithDefaults.Addresses[i].PreCommitControl == nil {
			curioConfigWithDefaults.Addresses[i].PreCommitControl = []string{}
		}
		if curioConfigWithDefaults.Addresses[i].CommitControl == nil {
			curioConfigWithDefaults.Addresses[i].CommitControl = []string{}
		}
		if curioConfigWithDefaults.Addresses[i].TerminateControl == nil {
			curioConfigWithDefaults.Addresses[i].TerminateControl = []string{}
		}
	}
	return meta, err
}
func GetConfig(ctx context.Context, layers []string, db *harmonydb.DB) (*config.CurioConfig, error) {
	curioConfig := config.DefaultCurioConfig()
	have := []string{}
	layers = append([]string{"base"}, layers...) // Always stack on top of "base" layer
	for _, layer := range layers {
		text := ""
		err := db.QueryRow(ctx, `SELECT config FROM harmony_config WHERE title=$1`, layer).Scan(&text)
		if err != nil {
			if strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
				return nil, fmt.Errorf("missing layer '%s' ", layer)
			}
			if layer == "base" {
				return nil, errors.New(`curio defaults to a layer named 'base'. 
				Either use 'migrate' command or edit a base.toml and upload it with: curio config set base.toml`)
			}
			return nil, fmt.Errorf("could not read layer '%s': %w", layer, err)
		}

		meta, err := LoadConfigWithUpgrades(text, curioConfig)
		if err != nil {
			return curioConfig, fmt.Errorf("could not read layer, bad toml %s: %w", layer, err)
		}
		for _, k := range meta.Keys() {
			have = append(have, strings.Join(k, " "))
		}
		log.Debugw("Using layer", "layer", layer, "config", curioConfig)
	}
	_ = have // FUTURE: verify that required fields are here.
	// If config includes 3rd-party config, consider JSONSchema as a way that
	// 3rd-parties can dynamically include config requirements and we can
	// validate the config. Because of layering, we must validate @ startup.
	return curioConfig, nil
}

func GetDefaultConfig(comment bool) (string, error) {
	c := config.DefaultCurioConfig()
	cb, err := config.ConfigUpdate(c, nil, config.Commented(comment), config.DefaultKeepUncommented(), config.NoEnv())
	if err != nil {
		return "", err
	}
	return string(cb), nil
}

func GetAPI(ctx context.Context, cctx *cli.Context) (*harmonydb.DB, *config.CurioConfig, api.Chain, jsonrpc.ClientCloser, error) {
	db, err := MakeDB(cctx)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	layers := cctx.StringSlice("layers")

	cfg, err := GetConfig(cctx.Context, layers, db)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	full, fullCloser, err := GetFullNodeAPIV1Curio(cctx, cfg.Apis.ChainApiInfo)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return db, cfg, full, fullCloser, nil
}
func GetDepsCLI(ctx context.Context, cctx *cli.Context) (*Deps, error) {
	db, cfg, full, fullCloser, err := GetAPI(ctx, cctx)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		fullCloser()
	}()

	return &Deps{
		Cfg:   cfg,
		DB:    db,
		Chain: full,
	}, nil
}

type CreateMinerConfigChainAPI interface {
	StateMinerInfo(context.Context, address.Address, types.TipSetKey) (lapi.MinerInfo, error)
}

func CreateMinerConfig(ctx context.Context, full CreateMinerConfigChainAPI, db *harmonydb.DB, miners []string, info string) error {
	var titles []string
	err := db.Select(ctx, &titles, `SELECT title FROM harmony_config WHERE LENGTH(config) > 0`)
	if err != nil {
		return fmt.Errorf("cannot reach the db. Ensure that Yugabyte flags are set correctly to"+
			" reach Yugabyte: %s", err.Error())
	}

	// setup config
	curioConfig := config.DefaultCurioConfig()

	for _, addr := range miners {
		maddr, err := address.NewFromString(addr)
		if err != nil {
			return xerrors.Errorf("Invalid address: %s", addr)
		}

		_, err = full.StateMinerInfo(ctx, maddr, types.EmptyTSK)
		if err != nil {
			return xerrors.Errorf("Failed to get miner info: %w", err)
		}

		curioConfig.Addresses = append(curioConfig.Addresses, config.CurioAddresses{
			PreCommitControl:      []string{},
			CommitControl:         []string{},
			TerminateControl:      []string{},
			DisableOwnerFallback:  false,
			DisableWorkerFallback: false,
			MinerAddresses:        []string{addr},
		})
	}

	{
		sk, err := io.ReadAll(io.LimitReader(rand.Reader, 32))
		if err != nil {
			return err
		}

		curioConfig.Apis.StorageRPCSecret = base64.StdEncoding.EncodeToString(sk)
	}

	{
		curioConfig.Apis.ChainApiInfo = append(curioConfig.Apis.ChainApiInfo, info)
	}

	curioConfig.Addresses = lo.Filter(curioConfig.Addresses, func(a config.CurioAddresses, _ int) bool {
		return len(a.MinerAddresses) > 0
	})

	// If no base layer is present
	if !lo.Contains(titles, "base") {
		cb, err := config.ConfigUpdate(curioConfig, config.DefaultCurioConfig(), config.Commented(true), config.DefaultKeepUncommented(), config.NoEnv())
		if err != nil {
			return xerrors.Errorf("Failed to generate default config: %w", err)
		}
		cfg := string(cb)
		_, err = db.Exec(ctx, "INSERT INTO harmony_config (title, config) VALUES ('base', $1)", cfg)
		if err != nil {
			return xerrors.Errorf("failed to insert the 'base' into the database: %w", err)
		}
		fmt.Printf("The base layer has been updated with miner[s] %s\n", miners)
		return nil
	}

	// if base layer is present
	baseCfg := config.DefaultCurioConfig()
	var baseText string
	err = db.QueryRow(ctx, "SELECT config FROM harmony_config WHERE title='base'").Scan(&baseText)
	if err != nil {
		return xerrors.Errorf("Cannot load base config from database: %w", err)
	}
	_, err = LoadConfigWithUpgrades(baseText, baseCfg)
	if err != nil {
		return xerrors.Errorf("Cannot parse base config: %w", err)
	}

	baseCfg.Addresses = append(baseCfg.Addresses, curioConfig.Addresses...)
	baseCfg.Addresses = lo.Filter(baseCfg.Addresses, func(a config.CurioAddresses, _ int) bool {
		return len(a.MinerAddresses) > 0
	})

	cb, err := config.ConfigUpdate(baseCfg, config.DefaultCurioConfig(), config.Commented(true), config.DefaultKeepUncommented(), config.NoEnv())
	if err != nil {
		return xerrors.Errorf("cannot interpret config: %w", err)
	}
	_, err = db.Exec(ctx, "UPDATE harmony_config SET config=$1 WHERE title='base'", string(cb))
	if err != nil {
		return xerrors.Errorf("cannot update base config: %w", err)
	}
	fmt.Printf("The base layer has been updated with miner[s] %s\n", miners)
	return nil
}
