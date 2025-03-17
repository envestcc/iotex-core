package factory

import (
	"sync"
	"time"

	"github.com/iotexproject/go-pkgs/hash"
	"go.uber.org/zap"

	"github.com/iotexproject/iotex-core/v2/blockchain/block"
	"github.com/iotexproject/iotex-core/v2/pkg/log"
)

type (
	blockPreparer[T any] struct {
		tasks map[hash.Hash256]chan *mintResult[T]
		mu    sync.Mutex
	}
	mintResult[T any] struct {
		blk T
		err error
	}
)

func newBlockPreparer() *blockPreparer[*block.Builder] {
	return &blockPreparer[*block.Builder]{
		tasks: make(map[hash.Hash256]chan *mintResult[*block.Builder]),
	}
}

func (d *blockPreparer[T]) PrepareBlock(prevHash []byte, timestamp time.Time, mintFn func() (T, error)) {
	d.mu.Lock()
	if _, ok := d.tasks[hash.BytesToHash256(prevHash)]; ok {
		log.L().Debug("draft block already exists", log.Hex("prevHash", prevHash))
		d.mu.Unlock()
		return
	}
	res := make(chan *mintResult[T], 1)
	d.tasks[hash.BytesToHash256(prevHash)] = res
	d.mu.Unlock()

	go func() {
		blk, err := mintFn()
		res <- &mintResult[T]{blk: blk, err: err}
		log.L().Debug("prepare mint returned", zap.Error(err))
	}()
}

func (d *blockPreparer[T]) WaitBlock(prevHash []byte, timestamp time.Time) (T, error) {
	var null T
	d.mu.Lock()
	hash := hash.Hash256(prevHash)
	if ch, ok := d.tasks[hash]; ok {
		d.mu.Unlock()
		res := <-ch
		d.mu.Lock()
		delete(d.tasks, hash)
		d.mu.Unlock()
		return res.blk, res.err
	}
	d.mu.Unlock()
	return null, nil
}

func (d *blockPreparer[T]) ReceiveBlock(blk *block.Block) error {
	d.mu.Lock()
	delete(d.tasks, blk.PrevHash())
	d.mu.Unlock()
	return nil
}
