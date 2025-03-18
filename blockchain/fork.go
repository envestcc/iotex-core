package blockchain

import (
	"github.com/iotexproject/go-pkgs/hash"
	"github.com/pkg/errors"

	"github.com/iotexproject/iotex-core/v2/blockchain/block"
	"github.com/iotexproject/iotex-core/v2/db"
)

type (
	blockchainFork struct {
		*blockchain
		head hash.Hash256
	}
)

func (fork *blockchainFork) BlockHeaderByHeight(height uint64) (*block.Header, error) {
	tip := fork.blockchain.TipHeight()
	if height <= tip {
		return fork.blockchain.BlockHeaderByHeight(height)
	}
	for blk := fork.bbf.Block(fork.head); blk != nil && blk.Height() > tip; blk = fork.bbf.Block(blk.PrevHash()) {
		if blk.Height() == height {
			return &blk.Header, nil
		}
	}
	return nil, errors.Wrap(db.ErrNotExist, "block header not found")
}

func (fork *blockchainFork) TipHash() hash.Hash256 {
	return fork.head
}

func (fork *blockchainFork) TipHeight() uint64 {
	return fork.bbf.Block(fork.head).Height()
}
