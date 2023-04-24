// Copyright (c) 2023 IoTeX Foundation
// This source code is provided 'as is' and no warranties are given as to title or non-infringement, merchantability
// or fitness for purpose and, to the extent permitted by law, all liability for your use of the code is disclaimed.
// This source code is governed by Apache License 2.0 that can be found in the LICENSE file.

package blockindex

import (
	"context"
	"encoding/binary"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/iotexproject/iotex-core/action"
	"github.com/iotexproject/iotex-core/action/protocol/staking"
	"github.com/iotexproject/iotex-core/blockchain/block"
	"github.com/iotexproject/iotex-core/blockchain/blockdao"
	"github.com/iotexproject/iotex-core/blockindex/indexpb"
	"github.com/iotexproject/iotex-core/pkg/util/byteutil"
	"github.com/iotexproject/iotex-proto/golang/iotextypes"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// TODO (iip-13): replace with the real liquid staking contract address
	LiquidStakingContractAddress = ""

	// TODO (iip-13): replace with the real liquid staking contract ABI
	_liquidStakingContractABI = `[
		{
			"anonymous": false,
			"inputs": [
				{
					"indexed": false,
					"internalType": "uint256",
					"name": "x",
					"type": "uint256"
				}
			],
			"name": "Set",
			"type": "event"
		},
		{
			"inputs": [],
			"name": "get",
			"outputs": [
				{
					"internalType": "uint256",
					"name": "",
					"type": "uint256"
				}
			],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [
				{
					"internalType": "uint256",
					"name": "x",
					"type": "uint256"
				}
			],
			"name": "set",
			"outputs": [],
			"stateMutability": "nonpayable",
			"type": "function"
		}
	]`

	// bucket related namespace in db
	_liquidStakingBucketInfoNS      = "lsbInfo"
	_liquidStakingBucketTypeNS      = "lsbType"
	_liquidStakingBucketTypeMapNS   = "lsbTypeMap"
	_liquidStakingBucketTypeCountNS = "lsbTypeCount"

	// bucket cache size
	_liquidStakingBucketCacheSize = 10000
)

type (
	// LiquidStakingIndexer is the interface of liquid staking indexer
	LiquidStakingIndexer interface {
		blockdao.BlockIndexer

		GetCandidateVotes(candidate string) (*big.Int, error)
		GetBucket(bucketIndex uint64) (*staking.VoteBucket, error)
	}

	liquidStakingIndexer struct {
		data *liquidStakingData

		blockInterval time.Duration
	}

	// BucketInfo is the bucket information
	BucketInfo struct {
		TypeIndex  uint64
		UnlockedAt *time.Time
		UnstakedAt *time.Time
		Delegate   string
	}

	// BucketType is the bucket type
	BucketType struct {
		Amount      *big.Int
		Duration    time.Duration
		ActivatedAt *time.Time
	}

	// eventParam is a struct to hold smart contract event parameters, which can easily convert a param to go type
	// TODO: this is general enough to be moved to a common package
	eventParam map[string]any
)

var (
	_liquidStakingInterface abi.ABI

	errInvlidEventParam   = errors.New("invalid event param")
	errBucketTypeNotExist = errors.New("bucket type does not exist")
	errBucketInfoNotExist = errors.New("bucket info does not exist")
)

func init() {
	var err error
	_liquidStakingInterface, err = abi.JSON(strings.NewReader(_liquidStakingContractABI))
	if err != nil {
		panic(err)
	}
}

// NewLiquidStakingIndexer creates a new liquid staking indexer
func NewLiquidStakingIndexer() *liquidStakingIndexer {
	return &liquidStakingIndexer{}
}

func (s *liquidStakingIndexer) Start(ctx context.Context) error {
	return nil
}

func (s *liquidStakingIndexer) Stop(ctx context.Context) error {
	return nil
}

func (s *liquidStakingIndexer) PutBlock(ctx context.Context, blk *block.Block) error {
	for _, receipt := range blk.Receipts {
		if receipt.Status != uint64(iotextypes.ReceiptStatus_Success) {
			continue
		}
		for _, log := range receipt.Logs() {
			if log.Address != LiquidStakingContractAddress {
				continue
			}
			if err := s.handleEvent(ctx, blk, log); err != nil {
				return err
			}
		}
	}
	return s.data.commit()
}

func (s *liquidStakingIndexer) DeleteTipBlock(context.Context, *block.Block) error {
	return nil
}

func (s *liquidStakingIndexer) Height() (uint64, error) {
	return 0, nil
}

func (s *liquidStakingIndexer) GetCandidateVotes(candidate string) (*big.Int, error) {
	return nil, nil
}

func (s *liquidStakingIndexer) GetBucket(bucketIndex uint64) (*staking.VoteBucket, error) {
	return nil, nil
}

func (s *liquidStakingIndexer) handleEvent(ctx context.Context, blk *block.Block, log *action.Log) error {
	// get event abi
	abiEvent, err := _liquidStakingInterface.EventByID(common.Hash(log.Topics[0]))
	if err != nil {
		return errors.Wrapf(err, "get event abi from topic %v failed", log.Topics[0])
	}

	// unpack event data
	event := make(eventParam)
	if err = abiEvent.Inputs.UnpackIntoMap(event, log.Data); err != nil {
		return errors.Wrap(err, "unpack event data failed")
	}

	// handle different kinds of event
	switch abiEvent.Name {
	case "BucketTypeActivated":
		err = s.handleBucketTypeActivatedEvent(event, blk.Timestamp())
	case "BucketTypeDeactivated":
		err = s.handleBucketTypeDeactivatedEvent(event)
	case "Staked":
		err = s.handleStakedEvent(event)
	case "Locked":
		err = s.handleLockedEvent(event)
	case "Unlocked":
		err = s.handleUnlockedEvent(event, blk.Timestamp())
	case "Unstaked":
		err = s.handleUnstakedEvent(event, blk.Timestamp())
	case "Merged":
		err = s.handleMergedEvent(event)
	case "DurationExtended":
		err = s.handleDurationExtendedEvent(event)
	case "AmountIncreased":
		err = s.handleAmountIncreasedEvent(event)
	case "DelegateChanged":
		err = s.handleDelegateChangedEvent(event)
	case "Withdrawal":
		err = s.handleWithdrawalEvent(event)
	default:
		err = nil
	}
	return err
}

func (s *liquidStakingIndexer) handleBucketTypeActivatedEvent(event eventParam, timeStamp time.Time) error {
	amountParam, err := event.fieldUint256("amount")
	if err != nil {
		return err
	}
	durationParam, err := event.fieldUint256("duration")
	if err != nil {
		return err
	}

	bt := BucketType{
		Amount:      amountParam,
		Duration:    s.blockHeightToDuration(durationParam.Uint64()),
		ActivatedAt: &timeStamp,
	}
	id, ok := s.data.getBucketTypeIndex(amountParam, bt.Duration)
	if !ok {
		id = s.data.getBucketTypeCount()
	}
	s.data.putBucketType(id, &bt)
	return nil
}

func (s *liquidStakingIndexer) handleBucketTypeDeactivatedEvent(event eventParam) error {
	amountParam, err := event.fieldUint256("amount")
	if err != nil {
		return err
	}
	durationParam, err := event.fieldUint256("duration")
	if err != nil {
		return err
	}

	id, ok := s.data.getBucketTypeIndex(amountParam, s.blockHeightToDuration(durationParam.Uint64()))
	if !ok {
		return errors.Wrapf(errBucketTypeNotExist, "amount %d, duration %d", amountParam.Int64(), durationParam.Uint64())
	}
	bt, ok := s.data.getBucketType(id)
	if !ok {
		return errors.Wrapf(errBucketTypeNotExist, "id %d", id)
	}
	bt.ActivatedAt = nil
	s.data.putBucketType(id, bt)
	return nil
}

func (s *liquidStakingIndexer) handleStakedEvent(event eventParam) error {
	tokenIDParam, err := event.fieldUint256("tokenId")
	if err != nil {
		return err
	}
	delegateParam, err := event.fieldBytes12("delegate")
	if err != nil {
		return err
	}
	amountParam, err := event.fieldUint256("amount")
	if err != nil {
		return err
	}
	durationParam, err := event.fieldUint256("duration")
	if err != nil {
		return err
	}

	btIdx, ok := s.data.getBucketTypeIndex(amountParam, s.blockHeightToDuration(durationParam.Uint64()))
	if !ok {
		return errors.Wrapf(errBucketTypeNotExist, "amount %d, duration %d", amountParam.Int64(), durationParam.Uint64())
	}
	bucket := BucketInfo{
		TypeIndex: btIdx,
		Delegate:  string(delegateParam[:]),
	}
	s.data.putBucketInfo(tokenIDParam.Uint64(), &bucket)
	return nil
}

func (s *liquidStakingIndexer) handleLockedEvent(event eventParam) error {
	tokenIDParam, err := event.fieldUint256("tokenId")
	if err != nil {
		return err
	}
	durationParam, err := event.fieldUint256("duration")
	if err != nil {
		return err
	}

	b, ok := s.data.getBucketInfo(tokenIDParam.Uint64())
	if !ok {
		return errors.Wrapf(errBucketInfoNotExist, "token id %d", tokenIDParam.Uint64())
	}
	bt, ok := s.data.getBucketType(b.TypeIndex)
	if !ok {
		return errors.Wrapf(errBucketTypeNotExist, "id %d", b.TypeIndex)
	}
	newBtIdx, ok := s.data.getBucketTypeIndex(bt.Amount, s.blockHeightToDuration(durationParam.Uint64()))
	if !ok {
		return errors.Wrapf(errBucketTypeNotExist, "amount %v, duration %d", bt.Amount, durationParam.Uint64())
	}
	b.TypeIndex = newBtIdx
	b.UnlockedAt = nil
	s.data.putBucketInfo(tokenIDParam.Uint64(), b)
	return nil
}

func (s *liquidStakingIndexer) handleUnlockedEvent(event eventParam, timestamp time.Time) error {
	tokenIDParam, err := event.fieldUint256("tokenId")
	if err != nil {
		return err
	}

	b, ok := s.data.getBucketInfo(tokenIDParam.Uint64())
	if !ok {
		return errors.Wrapf(errBucketInfoNotExist, "token id %d", tokenIDParam.Uint64())
	}
	b.UnlockedAt = &timestamp
	s.data.putBucketInfo(tokenIDParam.Uint64(), b)
	return nil
}

func (s *liquidStakingIndexer) handleUnstakedEvent(event eventParam, timestamp time.Time) error {
	tokenIDParam, err := event.fieldUint256("tokenId")
	if err != nil {
		return err
	}

	b, ok := s.data.getBucketInfo(tokenIDParam.Uint64())
	if !ok {
		return errors.Wrapf(errBucketInfoNotExist, "token id %d", tokenIDParam.Uint64())
	}
	b.UnstakedAt = &timestamp
	s.data.putBucketInfo(tokenIDParam.Uint64(), b)
	return nil
}

func (s *liquidStakingIndexer) handleMergedEvent(event eventParam) error {
	tokenIDsParam, err := event.fieldUint256Slice("tokenIds")
	if err != nil {
		return err
	}
	amountParam, err := event.fieldUint256("amount")
	if err != nil {
		return err
	}
	durationParam, err := event.fieldUint256("duration")
	if err != nil {
		return err
	}

	// merge to the first bucket
	btIdx, ok := s.data.getBucketTypeIndex(amountParam, s.blockHeightToDuration(durationParam.Uint64()))
	if !ok {
		return errors.Wrapf(errBucketTypeNotExist, "amount %d, duration %d", amountParam.Int64(), durationParam.Uint64())
	}
	b, ok := s.data.getBucketInfo(tokenIDsParam[0].Uint64())
	if !ok {
		return errors.Wrapf(errBucketInfoNotExist, "token id %d", tokenIDsParam[0].Uint64())
	}
	b.TypeIndex = btIdx
	b.UnlockedAt = nil
	for i := 1; i < len(tokenIDsParam); i++ {
		s.data.burnBucket(tokenIDsParam[i].Uint64())
	}
	s.data.putBucketInfo(tokenIDsParam[0].Uint64(), b)
	return nil
}

func (s *liquidStakingIndexer) handleDurationExtendedEvent(event eventParam) error {
	tokenIDParam, err := event.fieldUint256("tokenId")
	if err != nil {
		return err
	}
	durationParam, err := event.fieldUint256("duration")
	if err != nil {
		return err
	}

	b, ok := s.data.getBucketInfo(tokenIDParam.Uint64())
	if !ok {
		return errors.Wrapf(errBucketInfoNotExist, "token id %d", tokenIDParam.Uint64())
	}
	bt, ok := s.data.getBucketType(b.TypeIndex)
	if !ok {
		return errors.Wrapf(errBucketTypeNotExist, "id %d", b.TypeIndex)
	}
	newBtIdx, ok := s.data.getBucketTypeIndex(bt.Amount, s.blockHeightToDuration(durationParam.Uint64()))
	if !ok {
		return errors.Wrapf(errBucketTypeNotExist, "amount %d, duration %d", bt.Amount.Int64(), durationParam.Uint64())
	}
	b.TypeIndex = newBtIdx
	s.data.putBucketInfo(tokenIDParam.Uint64(), b)
	return nil
}

func (s *liquidStakingIndexer) handleAmountIncreasedEvent(event eventParam) error {
	tokenIDParam, err := event.fieldUint256("tokenId")
	if err != nil {
		return err
	}
	amountParam, err := event.fieldUint256("amount")
	if err != nil {
		return err
	}

	b, ok := s.data.getBucketInfo(tokenIDParam.Uint64())
	if !ok {
		return errors.Wrapf(errBucketInfoNotExist, "token id %d", tokenIDParam.Uint64())
	}
	bt, ok := s.data.getBucketType(b.TypeIndex)
	if !ok {
		return errors.Wrapf(errBucketTypeNotExist, "id %d", b.TypeIndex)
	}
	newBtIdx, ok := s.data.getBucketTypeIndex(amountParam, bt.Duration)
	if !ok {
		return errors.Wrapf(errBucketTypeNotExist, "amount %d, duration %d", amountParam.Int64(), bt.Duration)
	}
	b.TypeIndex = newBtIdx
	s.data.putBucketInfo(tokenIDParam.Uint64(), b)
	return nil
}

func (s *liquidStakingIndexer) handleDelegateChangedEvent(event eventParam) error {
	tokenIDParam, err := event.fieldUint256("tokenId")
	if err != nil {
		return err
	}
	delegateParam, err := event.fieldBytes12("newDelegate")
	if err != nil {
		return err
	}

	b, ok := s.data.getBucketInfo(tokenIDParam.Uint64())
	if !ok {
		return errors.Wrapf(errBucketInfoNotExist, "token id %d", tokenIDParam.Uint64())
	}
	b.Delegate = string(delegateParam[:])
	s.data.putBucketInfo(tokenIDParam.Uint64(), b)
	return nil
}

func (s *liquidStakingIndexer) handleWithdrawalEvent(event eventParam) error {
	tokenIDParam, err := event.fieldUint256("tokenId")
	if err != nil {
		return err
	}

	s.data.burnBucket(tokenIDParam.Uint64())
	return nil
}

func (s *liquidStakingIndexer) blockHeightToDuration(height uint64) time.Duration {
	return time.Duration(height) * s.blockInterval
}

func eventField[T any](e eventParam, name string) (T, error) {
	field, ok := e[name].(T)
	if !ok {
		return field, errors.Wrapf(errInvlidEventParam, "field %s got %#v, expect %T", name, e[name], field)
	}
	return field, nil
}

func (e eventParam) fieldUint256(name string) (*big.Int, error) {
	return eventField[*big.Int](e, name)
}

func (e eventParam) fieldBytes12(name string) ([12]byte, error) {
	return eventField[[12]byte](e, name)
}

func (e eventParam) fieldUint256Slice(name string) ([]*big.Int, error) {
	return eventField[[]*big.Int](e, name)
}

func (e eventParam) fieldAddress(name string) (common.Address, error) {
	return eventField[common.Address](e, name)
}

func (bt *BucketType) toProto() *indexpb.BucketType {
	return &indexpb.BucketType{
		Amount:      bt.Amount.String(),
		Duration:    uint64(bt.Duration),
		ActivatedAt: timestamppb.New(*bt.ActivatedAt),
	}
}

func (bt *BucketType) loadProto(p *indexpb.BucketType) error {
	var ok bool
	bt.Amount, ok = big.NewInt(0).SetString(p.Amount, 10)
	if !ok {
		return errors.New("failed to parse amount")
	}
	bt.Duration = time.Duration(p.Duration)
	t := p.ActivatedAt.AsTime()
	bt.ActivatedAt = &t
	return nil
}

func (bt *BucketType) serialize() []byte {
	return byteutil.Must(proto.Marshal(bt.toProto()))
}

func (bt *BucketType) deserialize(b []byte) error {
	m := indexpb.BucketType{}
	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}
	return bt.loadProto(&m)
}

func (bi *BucketInfo) toProto() *indexpb.BucketInfo {
	return &indexpb.BucketInfo{
		TypeIndex:  bi.TypeIndex,
		UnlockedAt: timestamppb.New(*bi.UnlockedAt),
		UnstakedAt: timestamppb.New(*bi.UnstakedAt),
		Delegate:   bi.Delegate,
	}
}

func (bi *BucketInfo) serialize() []byte {
	return byteutil.Must(proto.Marshal(bi.toProto()))
}

func (bi *BucketInfo) deserialize(b []byte) error {
	m := indexpb.BucketInfo{}
	if err := proto.Unmarshal(b, &m); err != nil {
		return err
	}
	return bi.loadProto(&m)
}

func (bi *BucketInfo) loadProto(p *indexpb.BucketInfo) error {
	bi.TypeIndex = p.TypeIndex
	t := p.UnlockedAt.AsTime()
	bi.UnlockedAt = &t
	t = p.UnstakedAt.AsTime()
	bi.UnstakedAt = &t
	bi.Delegate = p.Delegate
	return nil
}

func serializeUint64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func deserializeUint64(b []byte) uint64 {
	return binary.LittleEndian.Uint64(b)
}
