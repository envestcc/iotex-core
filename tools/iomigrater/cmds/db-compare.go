package cmd

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"

	"github.com/iotexproject/go-pkgs/cache"

	"github.com/iotexproject/iotex-core/action/protocol/execution/evm"
	"github.com/iotexproject/iotex-core/action/protocol/staking"
	"github.com/iotexproject/iotex-core/db"
	"github.com/iotexproject/iotex-core/db/batch"
	"github.com/iotexproject/iotex-core/pkg/util/byteutil"
	"github.com/iotexproject/iotex-core/state/factory"
	"github.com/iotexproject/iotex-core/tools/iomigrater/common"
)

// Multi-language support
var (
	//	stateDB2FactoryCmdShorts = map[string]string{
	//		"english": "Sub-Command for migration state db to factory db.",
	//		"chinese": "迁移 IoTeX 区块链 state db 到 factory db 的子命令",
	//	}
	//
	//	stateDB2FactoryCmdLongs = map[string]string{
	//		"english": "Sub-Command for migration state db to factory db.",
	//		"chinese": "迁移 IoTeX 区块链 state db 到 factory db 的子命令",
	//	}
	//
	dbCompareCmdUse = map[string]string{
		"english": "dbcompare",
		"chinese": "dbcompare",
	}

//	stateDB2FactoryFlagStateDBFileUse = map[string]string{
//		"english": "The statedb file you want to migrate.",
//		"chinese": "您要迁移的 statedb 文件。",
//	}
//
//	stateDB2FactoryFlagFactoryFileUse = map[string]string{
//		"english": "The path you want to migrate to",
//		"chinese": "您要迁移到的路径。",
//	}
//
//	stateDB2FactoryFlagPebbledbUse = map[string]string{
//		"english": "Output as pebbledb",
//		"chinese": "输出为 pebbledb",
//	}
)

var (
	// StateDB2Factory Used to Sub command.
	DBCompare = &cobra.Command{
		Use:   common.TranslateInLang(dbCompareCmdUse),
		Short: common.TranslateInLang(stateDB2FactoryCmdShorts),
		Long:  common.TranslateInLang(stateDB2FactoryCmdLongs),
		RunE: func(cmd *cobra.Command, args []string) error {
			return dbCompare()
		},
	}
)

var (
// statedbFile = ""
// factoryFile = ""
)

func init() {
	DBCompare.PersistentFlags().StringVarP(&statedbFile, "statedb", "s", "", common.TranslateInLang(stateDB2FactoryFlagStateDBFileUse))
	DBCompare.PersistentFlags().StringVarP(&factoryFile, "factory", "f", "", common.TranslateInLang(stateDB2FactoryFlagFactoryFileUse))
}

func dbCompare() (err error) {
	// Check flags
	if statedbFile == "" {
		return fmt.Errorf("--statedb is empty")
	}
	if factoryFile == "" {
		return fmt.Errorf("--factory is empty")
	}
	if statedbFile == factoryFile {
		return fmt.Errorf("the values of --statedb --factory flags cannot be the same")
	}

	// size := 200000
	statedb, err := bbolt.Open(statedbFile, 0666, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer statedb.Close()
	// read statedb height
	height := uint64(0)
	statedb.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(factory.AccountKVNamespace))
		if bucket == nil {
			return errors.New("bucket not found")
		}
		height = byteutil.BytesToUint64(bucket.Get([]byte(factory.CurrentHeightKey)))
		return nil
	})
	// open factorydb
	var factorydb db.KVStore
	factorydb, err = db.CreatePebbleKVStore(db.DefaultConfig, factoryFile)
	if err != nil {
		return errors.Wrap(err, "failed to create db")
	}
	if err = factorydb.Start(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start factory db")
	}
	defer func() {
		fmt.Printf("stop factorydb begin time %s\n", time.Now().Format(time.RFC3339))
		if e := factorydb.Stop(context.Background()); e != nil {
			fmt.Printf("failed to stop factorydb: %v\n", e)
		}
		fmt.Printf("stop factorydb end time %s\n", time.Now().Format(time.RFC3339))
	}()
	// open factory working set store
	preEaster := height < 4478761
	opts := []db.KVStoreFlusherOption{
		db.SerializeFilterOption(func(wi *batch.WriteInfo) bool {
			if wi.Namespace() == factory.ArchiveTrieNamespace {
				return true
			}
			if wi.Namespace() != evm.CodeKVNameSpace && wi.Namespace() != staking.CandsMapNS {
				return false
			}
			return preEaster
		}),
		db.SerializeOption(func(wi *batch.WriteInfo) []byte {
			if preEaster {
				return wi.SerializeWithoutWriteType()
			}
			return wi.Serialize()
		}),
	}
	flusher, err := db.NewKVStoreFlusher(
		factorydb,
		batch.NewCachedBatch(),
		opts...,
	)
	if err != nil {
		return err
	}
	wss, err := factory.NewFactoryWorkingSetStore(nil, flusher, cache.NewThreadSafeLruCache(1000))
	if err != nil {
		return err
	}
	if err = wss.Start(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start db for trie")
	}
	defer func() {
		fmt.Printf("stop wss begin time %s\n", time.Now().Format(time.RFC3339))
		if e := wss.Stop(context.Background()); e != nil {
			fmt.Printf("failed to stop wss: %v\n", e)
		}
		fmt.Printf("stop wss end time %s\n", time.Now().Format(time.RFC3339))
	}()

	if err := statedb.View(func(tx *bbolt.Tx) error {
		if err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if string(name) == factory.ArchiveTrieNamespace {
				fmt.Printf("skip namespace %s\n", name)
				return nil
			}
			fmt.Printf("migrating namespace: %s %d\n", name, b.Stats().KeyN)
			// bar := progressbar.NewOptions(b.Stats().KeyN, progressbar.OptionThrottle(time.Second))
			b.ForEach(func(k, v []byte) error {
				if v == nil {
					panic("unexpected nested bucket")
				}
				// if err := bar.Add(size); err != nil {
				// 	return errors.Wrap(err, "failed to add progress bar")
				// }
				var (
					value []byte
					err   error
				)
				if string(name) == factory.AccountKVNamespace && string(k) == factory.CurrentHeightKey {
					value, err = flusher.BaseKVStore().Get(string(name), k)
				} else {
					value, err = wss.Get(string(name), k)
				}
				if err != nil {
					fmt.Printf("ns %s key %x(%s) not found in factory\n", name, k, k)
					return nil
				}
				if !bytes.Equal(v, value) {
					fmt.Printf("ns %s key %x(%s) value mismatch\n", name, k, k)
					fmt.Printf("\tstatedb value %x\n", v)
					fmt.Printf("\tfactorydb value %x\n", value)
					return nil
				}
				return nil
			})
			// if err := bar.Finish(); err != nil {
			// 	return err
			// }
			fmt.Println()
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}
