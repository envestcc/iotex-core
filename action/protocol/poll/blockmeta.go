// Copyright (c) 2020 IoTeX Foundation
// This source code is provided 'as is' and no warranties are given as to title or non-infringement, merchantability
// or fitness for purpose and, to the extent permitted by law, all liability for your use of the code is disclaimed.
// This source code is governed by Apache License 2.0 that can be found in the LICENSE file.

package poll

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/iotexproject/iotex-address/address"

	"github.com/iotexproject/iotex-core/v2/action/protocol"
	"github.com/iotexproject/iotex-core/v2/action/protocol/poll/blockmetapb"
	"github.com/iotexproject/iotex-core/v2/pkg/log"
	"github.com/iotexproject/iotex-core/v2/state"
	"github.com/iotexproject/iotex-core/v2/systemcontracts"
)

// BlockMeta is a struct to store block metadata
type BlockMeta struct {
	Height   uint64
	Producer string
	MintTime time.Time
}

var _ protocol.ContractStorage = (*BlockMeta)(nil)

// NewBlockMeta constructs new blockmeta struct with given fieldss
func NewBlockMeta(height uint64, producer string, mintTime time.Time) *BlockMeta {
	return &BlockMeta{
		Height:   height,
		Producer: producer,
		MintTime: mintTime.UTC(),
	}
}

// Serialize serializes BlockMeta struct to bytes
func (bm *BlockMeta) Serialize() ([]byte, error) {
	pb, err := bm.Proto()
	if err != nil {
		return nil, err
	}
	return proto.Marshal(pb)
}

// Proto converts the BlockMeta struct to a protobuf message
func (bm *BlockMeta) Proto() (*blockmetapb.BlockMeta, error) {
	blkTime := timestamppb.New(bm.MintTime)
	return &blockmetapb.BlockMeta{
		BlockHeight:   bm.Height,
		BlockProducer: bm.Producer,
		BlockTime:     blkTime,
	}, nil
}

// Deserialize deserializes bytes to blockMeta
func (bm *BlockMeta) Deserialize(buf []byte) error {
	epochMetapb := &blockmetapb.BlockMeta{}
	if err := proto.Unmarshal(buf, epochMetapb); err != nil {
		return errors.Wrap(err, "failed to unmarshal blocklist")
	}
	return bm.LoadProto(epochMetapb)
}

// LoadProto loads blockMeta from proto
func (bm *BlockMeta) LoadProto(pb *blockmetapb.BlockMeta) error {
	if err := pb.GetBlockTime().CheckValid(); err != nil {
		return err
	}
	mintTime := pb.GetBlockTime().AsTime()
	bm.Height = pb.GetBlockHeight()
	bm.Producer = pb.GetBlockProducer()
	bm.MintTime = mintTime.UTC()
	return nil
}

func (bm *BlockMeta) storageContractAddress(ns string, key []byte) (address.Address, error) {
	if ns != protocol.SystemNamespace {
		return nil, errors.Errorf("invalid namespace %s, expected %s", ns, protocol.SystemNamespace)
	}
	return systemcontracts.SystemContracts[systemcontracts.PollBlockMetaContractIndex].Address, nil
}

func (bm *BlockMeta) storageContract(ns string, key []byte, backend systemcontracts.ContractBackend) (*systemcontracts.GenericStorageContract, error) {
	addr, err := bm.storageContractAddress(ns, key)
	if err != nil {
		return nil, err
	}
	contract, err := systemcontracts.NewGenericStorageContract(common.BytesToAddress(addr.Bytes()), backend)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create block meta storage contract")
	}
	return contract, nil
}

func (bm *BlockMeta) StoreToContract(ns string, key []byte, backend systemcontracts.ContractBackend) error {
	contract, err := bm.storageContract(ns, key, backend)
	if err != nil {
		return err
	}
	data, err := bm.Serialize()
	if err != nil {
		return errors.Wrap(err, "failed to serialize block meta")
	}
	if err := contract.Put(key, systemcontracts.GenericValue{PrimaryData: data}); err != nil {
		return errors.Wrapf(err, "failed to store block meta to contract %s", contract.Address().Hex())
	}
	return nil
}

func (bm *BlockMeta) LoadFromContract(ns string, key []byte, backend systemcontracts.ContractBackend) error {
	contract, err := bm.storageContract(ns, key, backend)
	if err != nil {
		return err
	}
	value, err := contract.Get(key)
	if err != nil {
		return errors.Wrapf(err, "failed to get block meta from contract %s with key %x", contract.Address().Hex(), key)
	}
	if !value.KeyExists {
		return errors.Wrapf(state.ErrStateNotExist, "block meta does not exist in contract %s with key %x", contract.Address().Hex(), key)
	}
	if err := bm.Deserialize(value.Value.PrimaryData); err != nil {
		return errors.Wrap(err, "failed to deserialize block meta")
	}
	return nil
}

func (bm *BlockMeta) DeleteFromContract(ns string, key []byte, backend systemcontracts.ContractBackend) error {
	contract, err := bm.storageContract(ns, key, backend)
	if err != nil {
		return err
	}
	if err := contract.Remove(key); err != nil {
		return errors.Wrapf(err, "failed to delete block meta from contract %s with key %x", contract.Address().Hex(), key)
	}
	return nil
}

func (bm *BlockMeta) ListFromContract(ns string, backend systemcontracts.ContractBackend) ([][]byte, []any, error) {
	return nil, nil, errors.New("not implemented")
}

func (bm *BlockMeta) BatchFromContract(ns string, keys [][]byte, backend systemcontracts.ContractBackend) ([]any, error) {
	contract, err := bm.storageContract(ns, nil, backend)
	if err != nil {
		return nil, err
	}
	storeResult, err := contract.BatchGet(keys)
	if err != nil {
		return nil, errors.Wrap(err, "failed to batch get vote buckets from contract")
	}
	results := make([]any, 0, len(storeResult.Values))
	for i, value := range storeResult.Values {
		if !storeResult.ExistsFlags[i] {
			results = append(results, nil)
			continue
		}
		blockMeta := &BlockMeta{}
		if err := blockMeta.Deserialize(value.PrimaryData); err != nil {
			return nil, errors.Wrapf(err, "failed to deserialize block meta %d", i)
		}
		results = append(results, blockMeta)
	}
	log.S().Debugf("Batch loaded %d block metas from contract %s with keys length %d", len(results), contract.Address().Hex(), len(keys))
	return results, nil
}
