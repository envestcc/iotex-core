package stakingindex

import "github.com/iotexproject/iotex-address/address"

type cache struct {
	buckets            map[uint64]*Bucket
	bucketsByCandidate map[string]map[uint64]bool
	totalBucketCount   uint64
}

func (s *cache) Copy() *cache {
	// TODO: implement this
	return nil
}

func (s *cache) PutBucket(id uint64, bkt *Bucket) error {
	// TODO: implement this
	return nil
}

func (s *cache) DeleteBucket(id uint64) error {
	// TODO: implement this
	return nil
}

func (s *cache) Buckets() ([]*Bucket, error) {
	// TODO: implement this
	return nil, nil
}

// Bucket returns the bucket
func (s *cache) Bucket(id uint64) (*Bucket, bool, error) {
	// TODO: implement this
	return nil, false, nil
}

// BucketsByIndices returns the buckets by indices
func (s *cache) BucketsByIndices(indices []uint64) ([]*Bucket, error) {
	// TODO: implement this
	return nil, nil
}

// BucketsByCandidate returns the buckets by candidate
func (s *cache) BucketsByCandidate(candidate address.Address) ([]*Bucket, error) {
	// TODO: implement this
	return nil, nil
}

// TotalBucketCount returns the total bucket count including active and burnt buckets
func (s *cache) TotalBucketCount() (uint64, error) {
	// TODO: implement this
	return 0, nil
}
