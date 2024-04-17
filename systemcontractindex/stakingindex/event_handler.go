package stakingindex

import (
	"context"

	"github.com/iotexproject/iotex-core/action"
	"github.com/iotexproject/iotex-core/blockchain/block"
	"github.com/iotexproject/iotex-core/db/batch"
)

type eventHandler struct {
	dirty *cache             // dirty cache, a view for current block
	delta batch.KVStoreBatch // delta for db to store buckets of current block
}

func newEventHandler(dirty *cache) *eventHandler {
	return &eventHandler{
		dirty: dirty,
		delta: batch.NewBatch(),
	}
}

func (eh *eventHandler) HandleEvent(ctx context.Context, blk *block.Block, log *action.Log) error {
	// TODO: implement this
	return nil
}

func (eh *eventHandler) Finalize() (batch.KVStoreBatch, *cache) {
	delta, dirty := eh.delta, eh.dirty
	eh.delta, eh.dirty = nil, nil
	return delta, dirty
}
