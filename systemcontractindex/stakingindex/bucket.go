package stakingindex

import (
	"math/big"

	"github.com/iotexproject/iotex-address/address"

	"github.com/iotexproject/iotex-core/action/protocol/staking"
)

type Bucket = staking.VoteBucket

func cloneBucket(b *Bucket) *Bucket {
	clone := &Bucket{
		Index:                     b.Index,
		Candidate:                 b.Candidate,
		Owner:                     b.Owner,
		StakedAmount:              b.StakedAmount,
		StakedDuration:            b.StakedDuration,
		CreateTime:                b.CreateTime,
		StakeStartTime:            b.StakeStartTime,
		UnstakeStartTime:          b.UnstakeStartTime,
		AutoStake:                 b.AutoStake,
		ContractAddress:           b.ContractAddress,
		StakedDurationBlockNumber: b.StakedDurationBlockNumber,
		CreateBlockHeight:         b.CreateBlockHeight,
		StakeStartBlockHeight:     b.StakeStartBlockHeight,
		UnstakeStartBlockHeight:   b.UnstakeStartBlockHeight,
	}
	candidate, _ := address.FromBytes(b.Candidate.Bytes())
	clone.Candidate = candidate
	owner, _ := address.FromBytes(b.Owner.Bytes())
	clone.Owner = owner
	stakingAmount := new(big.Int).Set(b.StakedAmount)
	clone.StakedAmount = stakingAmount
	return clone
}
