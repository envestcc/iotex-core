package cmd

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/iotexproject/iotex-core/tools/iomigrater/common"
)

// Multi-language support
var (
	compactPebbleDBCmdShorts = map[string]string{
		"english": "compact the pebble db file.",
		"chinese": "压缩 pebble 数据库文件。",
	}
	compactPebbleDBCmdUse = map[string]string{
		"english": "pebble-compact",
		"chinese": "pebble-compact",
	}
	compactPebbleDBCmdLongs = map[string]string{
		"english": "Sub-Command for compact the pebble db file.",
		"chinese": "压缩 pebble 数据库文件的子命令",
	}
	compactPebbleDBFlagFileUse = map[string]string{
		"english": "The db file you want to compact.",
		"chinese": "您要压缩的数据库文件。",
	}
)

var (
	// CompactPebbleDB Used to Sub command.
	CompactPebbleDB = &cobra.Command{
		Use:   common.TranslateInLang(compactPebbleDBCmdUse),
		Short: common.TranslateInLang(compactPebbleDBCmdShorts),
		Long:  common.TranslateInLang(compactPebbleDBCmdLongs),
		RunE: func(cmd *cobra.Command, args []string) error {
			return compactPebbleDBFile()
		},
	}
)

var (
	dbFile string
)

func init() {
	CompactPebbleDB.PersistentFlags().StringVarP(&dbFile, "file", "f", "", common.TranslateInLang(compactPebbleDBFlagFileUse))
}

func compactPebbleDBFile() (err error) {
	if dbFile == "" {
		return fmt.Errorf("--file is empty")
	}

	comparer := pebble.DefaultComparer
	comparer.Split = func(a []byte) int {
		return 8
	}
	db, err := pebble.Open(dbFile, &pebble.Options{
		Comparer:           comparer,
		FormatMajorVersion: pebble.FormatPrePebblev1MarkedCompacted,
	})
	if err != nil {
		return errors.Wrap(err, "failed to open pebble db")
	}
	defer db.Close()

	iter, err := db.NewIter(nil)
	if err != nil {
		return errors.Wrap(err, "failed to create iterator")
	}
	var first, last []byte
	if iter.First() {
		first = append(first, iter.Key()...)
	}
	if iter.Last() {
		last = append(last, iter.Key()...)
	}
	if err := iter.Close(); err != nil {
		return err
	}
	if err := db.Compact(first, last, true); err != nil {
		return errors.Wrap(err, "failed to compact db")
	}
	return nil
}
