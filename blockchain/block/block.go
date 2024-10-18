// Copyright (c) 2022 IoTeX Foundation
// This source code is provided 'as is' and no warranties are given as to title or non-infringement, merchantability
// or fitness for purpose and, to the extent permitted by law, all liability for your use of the code is disclaimed.
// This source code is governed by Apache License 2.0 that can be found in the LICENSE file.

package block

import (
	"context"
	"time"

	"github.com/iotexproject/go-pkgs/hash"
	"github.com/iotexproject/iotex-proto/golang/iotextypes"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/iotexproject/iotex-core/action"
	"github.com/iotexproject/iotex-core/endorsement"
	"github.com/iotexproject/iotex-core/pkg/log"
)

// Block defines the struct of block
type Block struct {
	Header
	Body
	Footer

	// TODO: move receipts out of block struct
	Receipts []*action.Receipt
}

// ConvertToBlockPb converts Block to Block
func (b *Block) ConvertToBlockPb() *iotextypes.Block {
	footer, err := b.ConvertToBlockFooterPb()
	if err != nil {
		log.L().Panic("failed to convert block footer to protobuf message")
	}
	return &iotextypes.Block{
		Header: b.Header.Proto(),
		Body:   b.Body.Proto(),
		Footer: footer,
	}
}

// Serialize returns the serialized byte stream of the block
func (b *Block) Serialize() ([]byte, error) {
	return proto.Marshal(b.ConvertToBlockPb())
}

// VerifyTxRoot verifies the transaction root hash
func (b *Block) VerifyTxRoot() error {
	root, err := b.CalculateTxRoot()
	if err != nil {
		log.L().Debug("error in getting hash", zap.Error(err))
		return err
	}
	if !b.Header.VerifyTransactionRoot(root) {
		return ErrTxRootMismatch
	}
	return nil
}

// RunnableActions abstructs RunnableActions from a Block.
func (b *Block) RunnableActions() RunnableActions {
	return RunnableActions{actions: b.Actions, txHash: b.txRoot}
}

// Finalize creates a footer for the block
func (b *Block) Finalize(endorsements []*endorsement.Endorsement, ts time.Time) error {
	if len(b.endorsements) != 0 {
		return errors.New("the block has been finalized")
	}
	b.endorsements = endorsements
	b.commitTime = ts

	return nil
}

// TransactionLog returns transaction logs in the block
func (b *Block) TransactionLog() *BlkTransactionLog {
	if len(b.Receipts) == 0 {
		return nil
	}

	blkLog := BlkTransactionLog{
		actionLogs: []*TransactionLog{},
	}
	for _, r := range b.Receipts {
		if log := ReceiptTransactionLog(r); log != nil {
			blkLog.actionLogs = append(blkLog.actionLogs, log)
		}
	}

	if len(blkLog.actionLogs) == 0 {
		return nil
	}
	return &blkLog
}

// ActionByHash returns the action of a given hash
func (b *Block) ActionByHash(h hash.Hash256) (*action.SealedEnvelope, uint32, error) {
	var (
		acts      = make(chan int, len(b.Actions))
		res       = make(chan int)
		wg, _     = errgroup.WithContext(context.Background())
		finish    = make(chan error, 1)
		workerNum = 8
	)

	for i := 0; i < workerNum; i++ {
		wg.Go(func() error {
			for index := range acts {
				act := b.Actions[index]
				actHash, err := act.Hash()
				if err != nil {
					return err
				}
				if actHash == h {
					res <- index
					return nil
				}
			}
			return nil
		})
	}
	for i := range b.Actions {
		acts <- i
	}

	go func() {
		finish <- wg.Wait()
	}()
	select {
	case idx := <-res:
		return b.Actions[idx], uint32(idx), nil
	case err := <-finish:
		if err != nil {
			return nil, 0, err
		}
		return nil, 0, errors.Errorf("block does not have action %x", h)
	}
}
