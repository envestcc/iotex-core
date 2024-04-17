package stakingindex

import (
	"math/big"

	"github.com/iotexproject/iotex-address/address"
)

type Bucket struct {
	Candidate                 address.Address
	Owner                     address.Address
	StakedAmount              *big.Int
	ContractAddress           string
	StakedDurationBlockNumber uint64
	CreatedAt                 uint64
	UnlockedAt                uint64
	UnstakedAt                uint64
}

func (b *Bucket) Clone() *Bucket {
	clone := &Bucket{
		StakedAmount:              b.StakedAmount,
		ContractAddress:           b.ContractAddress,
		StakedDurationBlockNumber: b.StakedDurationBlockNumber,
		CreatedAt:                 b.CreatedAt,
		UnlockedAt:                b.UnlockedAt,
		UnstakedAt:                b.UnstakedAt,
	}
	candidate, _ := address.FromBytes(b.Candidate.Bytes())
	clone.Candidate = candidate
	owner, _ := address.FromBytes(b.Owner.Bytes())
	clone.Owner = owner
	stakingAmount := new(big.Int).Set(b.StakedAmount)
	clone.StakedAmount = stakingAmount
	return clone
}

func (vb *Bucket) Deserialize(buf []byte) error {
	// TODO: implement this function
	return nil
}
