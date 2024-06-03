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

	"github.com/iotexproject/go-pkgs/hash"

	"github.com/iotexproject/iotex-core/db"
	"github.com/iotexproject/iotex-core/db/trie"
	"github.com/iotexproject/iotex-core/db/trie/mptrie"
	"github.com/iotexproject/iotex-core/state/factory"
	"github.com/iotexproject/iotex-core/tools/iomigrater/common"
)

var (
	// StateDB2Factory Used to Sub command.
	FactoryClient = &cobra.Command{
		Use:   "factoryclient",
		Short: "compare ns and tlt in factorydb based on statedb",
		Long:  "compare ns and tlt in factorydb based on statedb",
		RunE: func(cmd *cobra.Command, args []string) error {
			return factoryClient()
		},
	}
)

var (
	getKV = false
	putKV = false
)

func init() {
	FactoryClient.PersistentFlags().StringVarP(&factoryFile, "factory", "f", "", common.TranslateInLang(stateDB2FactoryFlagFactoryFileUse))
	FactoryClient.PersistentFlags().StringVarP(&statedbFile, "statedb", "s", "", common.TranslateInLang(stateDB2FactoryFlagStateDBFileUse))
	FactoryClient.PersistentFlags().StringSliceVarP(&namespaces, "namespaces", "n", []string{}, "namespaces to compare")
}

func factoryClient() error {
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
	// height := uint64(0)
	// statedb.View(func(tx *bbolt.Tx) error {
	// 	bucket := tx.Bucket([]byte(factory.AccountKVNamespace))
	// 	if bucket == nil {
	// 		return errors.New("bucket not found")
	// 	}
	// 	height = byteutil.BytesToUint64(bucket.Get([]byte(factory.CurrentHeightKey)))
	// 	return nil
	// })
	// open factorydb
	var factorydb db.KVStore
	dbCfg := db.DefaultConfig
	dbCfg.ReadOnly = true
	factorydb, err = db.CreatePebbleKVStore(dbCfg, factoryFile)
	if err != nil {
		return errors.Wrap(err, "failed to create db")
	}
	// if err = factorydb.Start(context.Background()); err != nil {
	// 	return errors.Wrap(err, "failed to start factory db")
	// }
	// defer func() {
	// 	fmt.Printf("stop factorydb begin time %s\n", time.Now().Format(time.RFC3339))
	// 	if e := factorydb.Stop(context.Background()); e != nil {
	// 		fmt.Printf("failed to stop factorydb: %v\n", e)
	// 	}
	// 	fmt.Printf("stop factorydb end time %s\n", time.Now().Format(time.RFC3339))
	// }()
	dbForTrie, err := trie.NewKVStore(factory.ArchiveTrieNamespace, factorydb)
	if err != nil {
		return errors.Wrap(err, "failed to create db for trie")
	}
	if err = dbForTrie.Start(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start db for trie")
	}
	tlt := mptrie.NewTwoLayerTrie(dbForTrie, nil, factory.ArchiveTrieRootKey)
	if err = tlt.Start(context.Background()); err != nil {
		return errors.Wrap(err, "failed to start tlt")
	}
	notfounds := [][][]byte{}
	unmatchs := [][][]byte{}
	if err := statedb.View(func(tx *bbolt.Tx) error {
		if err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
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
				var (
					value []byte
					err   error
				)
				nsHash := hash.Hash160b([]byte(name))
				keyLegacy := hash.Hash160b(k)
				if string(name) == factory.AccountKVNamespace && string(k) == factory.CurrentHeightKey {
					value, err = factorydb.Get(string(name), k)
				} else {
					value, err = tlt.Get(nsHash[:], keyLegacy[:])
					value2, err2 := factorydb.Get(string(name), k)
					if ((err == nil) != (err2 == nil)) || !bytes.Equal(value, value2) {
						fmt.Printf("\nns %s key %x value mismatch between ns and tlt\n", name, k)
						fmt.Printf("\tstatedb value %x, err %s\n", value, err)
						fmt.Printf("\tfactorydb value %x, err %s\n", value2, err2)
					}
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
