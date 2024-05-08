// Copyright (c) 2024 IoTeX Foundation
// This source code is provided 'as is' and no warranties are given as to title or non-infringement, merchantability
// or fitness for purpose and, to the extent permitted by law, all liability for your use of the code is disclaimed.
// This source code is governed by Apache License 2.0 that can be found in the LICENSE file.

package db

import (
	"context"

	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/iotexproject/iotex-core/db/batch"
	"github.com/iotexproject/iotex-core/pkg/lifecycle"
)

var (
	pebbledbMtc = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "iotex_pebbledb_metrics",
		Help: "pebbledb metrics.",
	}, []string{"type", "method"})
)

func init() {
	prometheus.MustRegister(pebbledbMtc)
}

// PebbleDB is KVStore implementation based on pebble DB
type PebbleDB struct {
	lifecycle.Readiness
	db     *pebble.DB
	path   string
	config Config
}

// NewBoltDB instantiates an BoltDB with implements KVStore
func NewPebbleDB(cfg Config) *PebbleDB {
	return &PebbleDB{
		db:     nil,
		path:   cfg.DbPath,
		config: cfg,
	}
}

// Start opens the DB (creates new file if not existing yet)
func (b *PebbleDB) Start(_ context.Context) error {
	db, err := pebble.Open(b.path, &pebble.Options{})
	if err != nil {
		return errors.Wrap(ErrIO, err.Error())
	}
	b.db = db
	return b.TurnOn()
}

// Stop closes the DB
func (b *PebbleDB) Stop(_ context.Context) error {
	if err := b.TurnOff(); err != nil {
		return err
	}
	if err := b.db.Close(); err != nil {
		return errors.Wrap(ErrIO, err.Error())
	}
	return nil
}

// Get retrieves a record
func (b *PebbleDB) Get(ns string, key []byte) ([]byte, error) {
	if !b.IsReady() {
		return nil, ErrDBNotStarted
	}
	v, closer, err := b.db.Get(nsKey(ns, key))
	if err != nil {
		return nil, err
	}
	val := make([]byte, len(v))
	copy(val, v)
	return val, closer.Close()
}

// Put inserts a <key, value> record
func (b *PebbleDB) Put(ns string, key, value []byte) (err error) {
	if !b.IsReady() {
		return ErrDBNotStarted
	}
	return b.db.Set(nsKey(ns, key), value, nil)
}

// Delete deletes a record,if key is nil,this will delete the whole bucket
func (b *PebbleDB) Delete(ns string, key []byte) (err error) {
	if !b.IsReady() {
		return ErrDBNotStarted
	}
	return b.db.Delete(nsKey(ns, key), nil)
}

// WriteBatch commits a batch
func (b *PebbleDB) WriteBatch(kvsb batch.KVStoreBatch) error {
	if !b.IsReady() {
		return ErrDBNotStarted
	}

	batch, err := b.dedup(kvsb)
	if err != nil {
		return nil
	}
	return batch.Commit(nil)
}

func (b *PebbleDB) dedup(kvsb batch.KVStoreBatch) (*pebble.Batch, error) {
	kvsb.Lock()
	defer kvsb.Unlock()

	type doubleKey struct {
		ns  string
		key string
	}
	// remove duplicate keys, only keep the last write for each key
	var (
		entryKeySet = make(map[doubleKey]struct{})
		ch          = b.db.NewBatch()
	)
	for i := kvsb.Size() - 1; i >= 0; i-- {
		write, e := kvsb.Entry(i)
		if e != nil {
			return nil, e
		}
		// only handle Put and Delete
		if write.WriteType() != batch.Put && write.WriteType() != batch.Delete {
			continue
		}
		key := write.Key()
		k := doubleKey{ns: write.Namespace(), key: string(key)}
		if _, ok := entryKeySet[k]; !ok {
			entryKeySet[k] = struct{}{}
			// add into batch
			if write.WriteType() == batch.Put {
				ch.Set(nsKey(write.Namespace(), key), write.Value(), nil)
			} else {
				ch.Delete(nsKey(write.Namespace(), key), nil)
			}
		}
	}
	return ch, nil
}

// Filter returns <k, v> pair in a bucket that meet the condition
func (b *PebbleDB) Filter(string, Condition, []byte, []byte) ([][]byte, [][]byte, error) {
	panic("not supported by PebbleDB")
}

func nsKey(ns string, key []byte) []byte {
	nk := []byte(ns)
	return append(nk, key...)
}
