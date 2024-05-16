package cmd

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"regexp"
	"slices"
	"time"

	"github.com/pkg/errors"
	"github.com/schollz/progressbar/v2"
	"github.com/spf13/cobra"
	"go.etcd.io/bbolt"

	"github.com/iotexproject/go-pkgs/cache"
	"github.com/iotexproject/go-pkgs/hash"

	"github.com/iotexproject/iotex-core/action/protocol/execution/evm"
	"github.com/iotexproject/iotex-core/action/protocol/staking"
	"github.com/iotexproject/iotex-core/db"
	"github.com/iotexproject/iotex-core/db/batch"
	"github.com/iotexproject/iotex-core/db/trie"
	"github.com/iotexproject/iotex-core/db/trie/mptrie"
	"github.com/iotexproject/iotex-core/pkg/util/byteutil"
	"github.com/iotexproject/iotex-core/state/factory"
	"github.com/iotexproject/iotex-core/tools/iomigrater/common"
)

// Multi-language support
var (
	stateDB2FactoryCmdShorts = map[string]string{
		"english": "Sub-Command for migration state db to factory db.",
		"chinese": "迁移 IoTeX 区块链 state db 到 factory db 的子命令",
	}
	stateDB2FactoryCmdLongs = map[string]string{
		"english": "Sub-Command for migration state db to factory db.",
		"chinese": "迁移 IoTeX 区块链 state db 到 factory db 的子命令",
	}
	stateDB2FactoryCmdUse = map[string]string{
		"english": "state2factory",
		"chinese": "state2factory",
	}
	stateDB2FactoryFlagStateDBFileUse = map[string]string{
		"english": "The statedb file you want to migrate.",
		"chinese": "您要迁移的 statedb 文件。",
	}
	stateDB2FactoryFlagFactoryFileUse = map[string]string{
		"english": "The path you want to migrate to",
		"chinese": "您要迁移到的路径。",
	}
	stateDB2FactoryFlagPebbledbUse = map[string]string{
		"english": "Output as pebbledb",
		"chinese": "输出为 pebbledb",
	}
)

var (
	// StateDB2Factory Used to Sub command.
	StateDB2Factory = &cobra.Command{
		Use:   common.TranslateInLang(stateDB2FactoryCmdUse),
		Short: common.TranslateInLang(stateDB2FactoryCmdShorts),
		Long:  common.TranslateInLang(stateDB2FactoryCmdLongs),
		RunE: func(cmd *cobra.Command, args []string) error {
			return statedb2Factory()
		},
	}
)

var (
	statedbFile = ""
	factoryFile = ""
	outAsPebble = false
	v2          = false
	namespaces  = []string{}
	trieMaxSize = 10000000
	notStatsNS  = []string{}
	diffFile    = ""
	diffStart   = 0
)

func init() {
	StateDB2Factory.PersistentFlags().StringVarP(&statedbFile, "statedb", "s", "", common.TranslateInLang(stateDB2FactoryFlagStateDBFileUse))
	StateDB2Factory.PersistentFlags().StringVarP(&factoryFile, "factory", "f", "", common.TranslateInLang(stateDB2FactoryFlagFactoryFileUse))
	StateDB2Factory.PersistentFlags().BoolVarP(&outAsPebble, "pebbledb", "p", false, "Output as pebbledb")
	StateDB2Factory.PersistentFlags().BoolVarP(&v2, "v2", "2", false, "Use workingSet to convert")
	StateDB2Factory.PersistentFlags().StringSliceVarP(&namespaces, "namespaces", "n", []string{}, "Namespaces to migrate")
	StateDB2Factory.PersistentFlags().IntVarP(&trieMaxSize, "trieMaxSize", "m", 10000000, "Max size of trie")
	StateDB2Factory.PersistentFlags().StringSliceVarP(&notStatsNS, "nostats", "", []string{}, "Namespaces not to stats")
	StateDB2Factory.PersistentFlags().StringVarP(&diffFile, "diff", "d", "", "Diff file")
	StateDB2Factory.PersistentFlags().IntVarP(&diffStart, "diffstart", "", 0, "Diff start")
}

func statedb2Factory() (err error) {
	// Start pprof
	go func() {
		http.ListenAndServe("0.0.0.0:6060", nil)
	}()

	if v2 {
		return statedb2FactoryV2()
	}
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

	size := 200000
	statedb, err := bbolt.Open(statedbFile, 0666, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return err
	}
	defer statedb.Close()

	var factorydb db.KVStore
	if outAsPebble {
		fmt.Printf("output as pebbledb\n")
		factorydb, err = db.CreatePebbleKVStore(db.DefaultConfig, factoryFile)
	} else {
		fmt.Printf("output as boltdb\n")
		factorydb, err = db.CreateKVStore(db.DefaultConfig, factoryFile)
	}
	if err != nil {
		return errors.Wrap(err, "failed to create db")
	}
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

	bat := batch.NewBatch()
	height := uint64(0)
	realWriteCount := uint64(0)
	writeBatch := func(bat batch.KVStoreBatch) error {
		if err = factorydb.WriteBatch(bat); err != nil {
			return errors.Wrap(err, "failed to write batch")
		}
		for i := 0; i < bat.Size(); i++ {
			e, err := bat.Entry(i)
			if err != nil {
				return errors.Wrap(err, "failed to get entry")
			}
			nsHash := hash.Hash160b([]byte(e.Namespace()))
			keyLegacy := hash.Hash160b(e.Key())
			if e.Namespace() == factory.AccountKVNamespace && string(e.Key()) == factory.CurrentHeightKey {
				height = byteutil.BytesToUint64(e.Value())
			} else {
				if err = tlt.Upsert(nsHash[:], keyLegacy[:], e.Value()); err != nil {
					return errors.Wrap(err, "failed to upsert tlt")
				}
			}
			realWriteCount++
		}
		if _, err := tlt.RootHash(); err != nil {
			return errors.Wrap(err, "failed to get root hash")
		}
		return nil
	}
	if err := statedb.View(func(tx *bbolt.Tx) error {
		if err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if len(namespaces) > 0 && slices.Index(namespaces, string(name)) < 0 {
				fmt.Printf("skip ns %s\n", name)
				return nil
			}
			if string(name) == factory.ArchiveTrieNamespace {
				fmt.Printf("skip ns %s\n", name)
				return nil
			}
			keyNum := b.Stats().KeyN
			fmt.Printf("migrating namespace: %s %d\n", name, keyNum)
			bar := progressbar.NewOptions(keyNum, progressbar.OptionThrottle(time.Second))
			err = b.ForEach(func(k, v []byte) error {
				if v == nil {
					panic("unexpected nested bucket")
				}
				bat.Put(string(name), k, v, "failed to put")
				if uint32(bat.Size()) >= uint32(size) {
					if err = bar.Add(slices.Min([]int{bat.Size(), keyNum})); err != nil {
						fmt.Printf("failed to add progress bar %v\n", err)
					}
					if err = writeBatch(bat); err != nil {
						return err
					}
					bat = batch.NewBatch()
				}
				return nil
			})
			if err != nil {
				return err
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
	if bat.Size() > 0 {
		fmt.Printf("write the last batch %d\n", bat.Size())
		if err := writeBatch(bat); err != nil {
			return err
		}
	}
	fmt.Printf("real write count %d\n", realWriteCount)
	// finalize
	rootHash, err := tlt.RootHash()
	if err != nil {
		return err
	}
	fmt.Printf("finalize height %d, root %x\n", height, rootHash)
	if err = factorydb.Put(factory.AccountKVNamespace, []byte(factory.CurrentHeightKey), byteutil.Uint64ToBytes(height)); err != nil {
		return errors.Wrap(err, "failed to put height")
	}
	if err = factorydb.Put(factory.ArchiveTrieNamespace, []byte(factory.ArchiveTrieRootKey), rootHash); err != nil {
		return errors.Wrap(err, "failed to put root hash")
	}
	// Persist the historical accountTrie's root hash
	if err = factorydb.Put(
		factory.ArchiveTrieNamespace,
		[]byte(fmt.Sprintf("%s-%d", factory.ArchiveTrieRootKey, height)),
		rootHash,
	); err != nil {
		return errors.Wrap(err, "failed to put historical root hash")
	}
	if err = tlt.SetRootHash(rootHash); err != nil {
		return errors.Wrap(err, "failed to set root hash")
	}
	fmt.Printf("stop tlt\n")
	if err = tlt.Stop(context.Background()); err != nil {
		return errors.Wrap(err, "failed to stop tlt")
	}
	fmt.Printf("stop db\n")
	if err = dbForTrie.Stop(context.Background()); err != nil {
		return errors.Wrap(err, "failed to stop db for trie")
	}
	return nil
}

func statedb2FactoryV2() (err error) {
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

	size := 200000
	// open statedb
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
	if outAsPebble {
		fmt.Printf("output as pebbledb\n")
		factorydb, err = db.CreatePebbleKVStore(db.DefaultConfig, factoryFile)
	} else {
		fmt.Printf("output as boltdb\n")
		factorydb, err = db.CreateKVStore(db.DefaultConfig, factoryFile)
	}
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
	wssCache := cache.NewThreadSafeLruCache(1000)
	wss, err := factory.NewFactoryWorkingSetStore(nil, flusher, wssCache)
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

	trieSize := uint64(0)
	bat := batch.NewBatch()
	writeBatch := func(bat batch.KVStoreBatch) error {
		// if err = factorydb.WriteBatch(bat); err != nil {
		// 	return errors.Wrap(err, "failed to write batch")
		// }
		trieSize += uint64(bat.Size())
		for i := 0; i < bat.Size(); i++ {
			e, err := bat.Entry(i)
			if err != nil {
				return errors.Wrap(err, "failed to get entry")
			}
			if e.Namespace() == factory.AccountKVNamespace && string(e.Key()) == factory.CurrentHeightKey {
				height = byteutil.BytesToUint64(e.Value())
			} else {
				wss.Put(e.Namespace(), e.Key(), e.Value())
			}
		}
		if err = wss.Finalize(height); err != nil {
			return errors.Wrap(err, "failed to finalize")
		}
		if err = wss.Commit(); err != nil {
			return errors.Wrap(err, "failed to commit")
		}
		if trieSize >= uint64(trieMaxSize) {
			if err = wss.Stop(context.Background()); err != nil {
				return errors.Wrap(err, "failed to stop wss")
			}
			wss, err = factory.NewFactoryWorkingSetStore(nil, flusher, wssCache)
			if err != nil {
				return err
			}
			if err = wss.Start(context.Background()); err != nil {
				return errors.Wrap(err, "failed to start db for trie")
			}
			trieSize = 0
		}
		return nil
	}
	if err := statedb.View(func(tx *bbolt.Tx) error {
		if len(diffFile) > 0 {
			bar := progressbar.NewOptions(1000000, progressbar.OptionThrottle(time.Millisecond*100), progressbar.OptionShowCount(), progressbar.OptionSetRenderBlankState(true))
			index := 0
			err := foreachFile(diffFile, func(d *diff) error {
				index++
				if index < diffStart {
					return nil
				}
				bt := tx.Bucket([]byte(d.ns))
				if bt == nil {
					return errors.Errorf("bucket not found: %s", d.ns)
				}
				val := bt.Get(d.key)
				if val == nil {
					return errors.Errorf("key not found: ns %s key %x", d.ns, d.key)
				}
				bat.Put(d.ns, d.key, val, "failed to put")
				if bat.Size() >= size {
					if index >= bar.GetMax() {
						bar.ChangeMax(index * 3)
					}
					bar.Add(bat.Size())
					if err := writeBatch(bat); err != nil {
						return err
					}
					bat = batch.NewBatch()
				}
				return nil
			})
			if err != nil {
				return err
			}
			bar.ChangeMax(index)
			if bat.Size() > 0 {
				if err := writeBatch(bat); err != nil {
					return err
				}
			}
			bar.Finish()
		} else if err := tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			if len(namespaces) > 0 && slices.Index(namespaces, string(name)) < 0 {
				fmt.Printf("skip ns %s\n", name)
				return nil
			}
			if string(name) == factory.ArchiveTrieNamespace {
				fmt.Printf("skip ns %s\n", name)
				return nil
			}
			keyNum := 1000000
			noStats := slices.Index(notStatsNS, string(name)) >= 0
			if !noStats {
				keyNum = b.Stats().KeyN
				fmt.Printf("migrating namespace: %s %d\n", name, keyNum)
			} else {
				fmt.Printf("migrating namespace: %s unknown\n", name)
			}
			bar := progressbar.NewOptions(keyNum, progressbar.OptionThrottle(time.Millisecond*100), progressbar.OptionShowCount(), progressbar.OptionSetRenderBlankState(true))
			realKeyNum := 0
			err = b.ForEach(func(k, v []byte) error {
				if v == nil {
					panic("unexpected nested bucket")
				}
				realKeyNum++
				bat.Put(string(name), k, v, "failed to put")
				if uint32(bat.Size()) >= uint32(size) {
					if noStats && realKeyNum >= bar.GetMax() {
						bar.ChangeMax(realKeyNum * 3)
					}
					if err := bar.Add(bat.Size()); err != nil {
						fmt.Printf("failed to add progress bar %v\n", err)
					}
					if err = writeBatch(bat); err != nil {
						return err
					}
					bat = batch.NewBatch()
				}
				return nil
			})
			if err != nil {
				return err
			}
			if noStats {
				bar.ChangeMax(realKeyNum)
			}
			// write bucket remaining data
			if err := bar.Add(bat.Size()); err != nil {
				fmt.Printf("failed to add progress bar %v\n", err)
			}
			if err = writeBatch(bat); err != nil {
				return err
			}
			bat = batch.NewBatch()
			if err := bar.Finish(); err != nil {
				fmt.Printf("failed to finish progress bar %v\n", err)
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
	if bat.Size() > 0 {
		fmt.Printf("write the last batch %d\n", bat.Size())
		if err := writeBatch(bat); err != nil {
			return err
		}
	}
	// finalize
	if err := wss.Finalize(height); err != nil {
		return errors.Wrap(err, "failed to finalize")
	}
	if err := wss.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit")
	}
	return nil
}

type diff struct {
	ns  string
	key []byte
}

func foreachFile(path string, fn func(*diff) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return foreachReader(file, fn)
}

func foreachReader(r io.Reader, fn func(*diff) error) error {
	scanner := bufio.NewScanner(r)
	// Set the maximum token size to 1GB.
	const maxTokenSize = 1 * 1024 * 1024 * 1024
	buf := make([]byte, maxTokenSize)
	scanner.Buffer(buf, maxTokenSize)

	re := regexp.MustCompile(`(not found|unmatch): ns (\S+) key (\S+)`)

	for scanner.Scan() {
		line := scanner.Text()
		match := re.FindStringSubmatch(line)
		if match != nil {
			key, err := hex.DecodeString(match[3])
			if err != nil {
				return err
			}
			if err = fn(&diff{ns: match[2], key: key}); err != nil {
				return err
			}
		}
	}

	return scanner.Err()
}
