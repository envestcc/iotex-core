package cmd

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/schollz/progressbar/v2"
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
	DBCompare.PersistentFlags().StringSliceVarP(&namespaces, "namespaces", "n", []string{}, "namespaces to compare")
	DBCompare.PersistentFlags().IntVarP(&trieMaxSize, "trieMaxSize", "m", 10000000, "Max size of trie")
	DBCompare.PersistentFlags().StringSliceVarP(&notStatsNS, "nostats", "", []string{}, "Namespaces not to stats")
	DBCompare.PersistentFlags().StringVarP(&diffFile, "diff", "d", "", "Diff file")
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
	notfounds := [][][]byte{}
	unmatchs := [][][]byte{}
	trieSize := uint64(0)
	if err := statedb.View(func(tx *bbolt.Tx) error {
		if len(diffFile) > 0 {
			bar := progressbar.NewOptions(1000000, progressbar.OptionThrottle(time.Millisecond*100), progressbar.OptionShowCount(), progressbar.OptionSetRenderBlankState(true))
			index := 0
			err := foreachFile(diffFile, func(d *diff) error {
				index++
				if index >= bar.GetMax() {
					bar.ChangeMax(index * 3)
				}
				if err := bar.Add(1); err != nil {
					fmt.Printf("failed to update processbar %s\n", err)
				}
				if index < diffStart {
					return nil
				}
				if trieSize > uint64(trieMaxSize) {
					if err = wss.Stop(context.Background()); err != nil {
						return errors.Wrap(err, "failed to stop db for trie")
					}
					wss, err := factory.NewFactoryWorkingSetStore(nil, flusher, cache.NewThreadSafeLruCache(1000))
					if err != nil {
						return err
					}
					if err = wss.Start(context.Background()); err != nil {
						return errors.Wrap(err, "failed to start db for trie")
					}
					trieSize = 0
				}
				bt := tx.Bucket([]byte(d.ns))
				if bt == nil {
					return errors.Errorf("bucket not found: %s", d.ns)
				}
				val := bt.Get(d.key)
				if val == nil {
					return errors.Errorf("key not found: ns %s key %x", d.ns, d.key)
				}
				trieSize++
				val2, err := wss.Get(d.ns, d.key)
				if err != nil {
					if !errors.Is(err, db.ErrNotExist) {
						fmt.Printf("ns %s key %x get error %s\n", d.ns, d.key, err)
					}
					notfounds = append(notfounds, [][]byte{[]byte(d.ns), d.key})
					return nil
				}
				if !bytes.Equal(val, val2) {
					unmatchs = append(unmatchs, [][]byte{[]byte(d.ns), d.key, val})
					return nil
				}
				return nil
			})
			if err != nil {
				return err
			}
			bar.ChangeMax(index)
			bar.Finish()
			fmt.Println()
		} else if err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if len(namespaces) > 0 && slices.Index(namespaces, string(name)) < 0 {
				fmt.Printf("skip ns %s\n", name)
				return nil
			}
			if string(name) == factory.ArchiveTrieNamespace {
				fmt.Printf("skip namespace %s\n", name)
				return nil
			}
			keyNum := 1000000
			noStats := slices.Index(notStatsNS, string(name)) >= 0
			if !noStats {
				keyNum = b.Stats().KeyN
				fmt.Printf("compare namespace: %s %d\n", name, keyNum)
			} else {
				fmt.Printf("compare namespace: %s unknown\n", name)
			}
			bar := progressbar.NewOptions(keyNum, progressbar.OptionThrottle(time.Millisecond*100), progressbar.OptionShowCount(), progressbar.OptionSetRenderBlankState(true))
			realKeyNum := 0
			err = b.ForEach(func(k, v []byte) error {
				if v == nil {
					panic("unexpected nested bucket")
				}
				realKeyNum++
				if noStats && realKeyNum >= bar.GetMax() {
					bar.ChangeMax(realKeyNum * 3)
				}
				if err := bar.Add(1); err != nil {
					fmt.Printf("failed to update processbar %s\n", err)
				}
				trieSize++
				if trieSize > uint64(trieMaxSize) {
					if err = wss.Stop(context.Background()); err != nil {
						return errors.Wrap(err, "failed to stop db for trie")
					}
					wss, err := factory.NewFactoryWorkingSetStore(nil, flusher, cache.NewThreadSafeLruCache(1000))
					if err != nil {
						return err
					}
					if err = wss.Start(context.Background()); err != nil {
						return errors.Wrap(err, "failed to start db for trie")
					}
					trieSize = 0
				}
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
					// fmt.Printf("ns %s key %x not found in factory\n", name, k)
					notfounds = append(notfounds, [][]byte{name, k})
					return nil
				}
				if !bytes.Equal(v, value) {
					unmatchs = append(unmatchs, [][]byte{name, k, v})
					// fmt.Printf("ns %s key %x value mismatch\n", name, k)
					// fmt.Printf("\tstatedb value %x\n", v)
					// fmt.Printf("\tfactorydb value %x\n", value)
					return nil
				}
				return nil
			})
			if err != nil {
				return err
			}
			if noStats {
				bar.ChangeMax(realKeyNum)
			}
			if err := bar.Finish(); err != nil {
				return err
			}
			fmt.Println()
			return nil
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// print notfounds and unmatchs
	fmt.Printf("not founds %d:\n", len(notfounds))
	for _, nf := range notfounds {
		fmt.Printf("not found: ns %s key %x\n", nf[0], nf[1])
	}
	fmt.Printf("unmatchs %d:\n", len(unmatchs))
	for _, um := range unmatchs {
		fmt.Printf("unmatch: ns %s key %x value %x\n", um[0], um[1], um[2])
	}
	return nil
}
