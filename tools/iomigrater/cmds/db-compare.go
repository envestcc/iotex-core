package cmd

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"

	"github.com/iotexproject/iotex-core/db"
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

	comparer := pebble.DefaultComparer
	comparer.Split = func(a []byte) int {
		return 8
	}
	cfg := db.DefaultConfig
	cfg.DbPath = factoryFile
	cfg.ReadOnly = true
	factoryDB := db.NewPebbleDB(cfg)
	if err := factoryDB.Start(context.Background()); err != nil {
		return err
	}

	if err := statedb.View(func(tx *bbolt.Tx) error {
		if err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			fmt.Printf("migrating namespace: %s %d\n", name, b.Stats().KeyN)
			if string(name) != factory.ArchiveTrieNamespace {
				fmt.Printf("skip\n")
				return nil
			}
			// bar := progressbar.NewOptions(b.Stats().KeyN, progressbar.OptionThrottle(time.Second))
			b.ForEach(func(k, v []byte) error {
				if v == nil {
					panic("unexpected nested bucket")
				}
				// if err := bar.Add(size); err != nil {
				// 	return errors.Wrap(err, "failed to add progress bar")
				// }
				value, err := factoryDB.Get(string(name), k)
				if err != nil {
					fmt.Printf("key %x not found\n", k)
					return nil
				}
				if !bytes.Equal(v, value) {
					fmt.Printf("key %x value mismatch\n", k)
					fmt.Printf("statedb value %x\n", v)
					fmt.Printf("factorydb value %x\n", value)
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
	fmt.Printf("stop db\n")
	if err = factoryDB.Stop(context.Background()); err != nil {
		return errors.Wrap(err, "failed to stop db for trie")
	}
	return nil
}
