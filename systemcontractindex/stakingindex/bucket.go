package stakingindex

import (
	"math/big"
	"time"

	"github.com/iotexproject/iotex-address/address"

	"github.com/iotexproject/iotex-core/action/protocol/staking"
)

type VoteBucket = staking.VoteBucket

type Bucket struct {
	Candidate                 address.Address
	Owner                     address.Address
	StakedAmount              *big.Int
	StakedDurationBlockNumber uint64
	CreatedAt                 uint64
	UnlockedAt                uint64
	UnstakedAt                uint64
}

func (b *Bucket) Clone() *Bucket {
	clone := &Bucket{
		StakedAmount:              b.StakedAmount,
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

func assembleVoteBucket(token uint64, bkt *Bucket, contractAddr string, blockInterval time.Duration) *VoteBucket {
	vb := VoteBucket{
		Index:                     token,
		StakedAmount:              bkt.StakedAmount,
		StakedDuration:            time.Duration(bkt.StakedDurationBlockNumber) * blockInterval,
		StakedDurationBlockNumber: bkt.StakedDurationBlockNumber,
		CreateBlockHeight:         bkt.CreatedAt,
		StakeStartBlockHeight:     bkt.CreatedAt,
		UnstakeStartBlockHeight:   bkt.UnstakedAt,
		AutoStake:                 bkt.UnlockedAt == maxBlockNumber,
		Candidate:                 bkt.Candidate,
		Owner:                     bkt.Owner,
		ContractAddress:           contractAddr,
	}
	if bkt.UnlockedAt != maxBlockNumber {
		vb.StakeStartBlockHeight = bkt.UnlockedAt
	}
	return &vb
}

func batchAssembleVoteBucket(idxs []uint64, bkts []*Bucket, contractAddr string, blockInterval time.Duration) []*VoteBucket {
	vbs := make([]*VoteBucket, 0, len(idxs))
	for i := range idxs {
		if bkts[i] == nil {
			vbs = append(vbs, nil)
			continue
		}
		vbs = append(vbs, assembleVoteBucket(idxs[i], bkts[i], contractAddr, blockInterval))
	}
	return vbs
}
