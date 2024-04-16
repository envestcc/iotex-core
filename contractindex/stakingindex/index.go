package stakingindex

import (
	"context"

	"github.com/iotexproject/iotex-address/address"

	"github.com/iotexproject/iotex-core/blockchain/block"
	"github.com/iotexproject/iotex-core/db"
)

type (
	Indexer struct {
		kvstore db.KVStore // persistent storage
		cache   *cache     // in-memory cache, used to query index data
		config  *Config
	}

	Config struct {
		ContractAddress      string
		ContractDeployHeight uint64
	}
)

// Start starts the indexer
func (s *Indexer) Start(ctx context.Context) error {
	// TODO: implement this
	return nil
}

// Stop stops the indexer
func (s *Indexer) Stop(ctx context.Context) error {
	// TODO: implement this
	return nil
}

// Height returns the tip block height
func (s *Indexer) Height() (uint64, error) {
	return 0, nil
}

// StartHeight returns the start height of the indexer
func (s *Indexer) StartHeight() uint64 {
	// TODO: implement this
	return 0
}

// Buckets returns the buckets
func (s *Indexer) Buckets(height uint64) ([]*Bucket, error) {
	// TODO: implement this
	return nil, nil
}

// Bucket returns the bucket
func (s *Indexer) Bucket(id uint64, height uint64) (*Bucket, bool, error) {
	// TODO: implement this
	return nil, false, nil
}

// BucketsByIndices returns the buckets by indices
func (s *Indexer) BucketsByIndices(indices []uint64, height uint64) ([]*Bucket, error) {
	// TODO: implement this
	return nil, nil
}

// BucketsByCandidate returns the buckets by candidate
func (s *Indexer) BucketsByCandidate(candidate address.Address, height uint64) ([]*Bucket, error) {
	// TODO: implement this
	return nil, nil
}

// TotalBucketCount returns the total bucket count including active and burnt buckets
func (s *Indexer) TotalBucketCount(height uint64) (uint64, error) {
	// TODO: implement this
	return 0, nil
}

// PutBlock puts a block into indexer
func (s *Indexer) PutBlock(ctx context.Context, blk *block.Block) error {
	return nil
}
