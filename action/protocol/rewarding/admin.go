// Copyright (c) 2019 IoTeX Foundation
// This source code is provided 'as is' and no warranties are given as to title or non-infringement, merchantability
// or fitness for purpose and, to the extent permitted by law, all liability for your use of the code is disclaimed.
// This source code is governed by Apache License 2.0 that can be found in the LICENSE file.

package rewarding

import (
	"bytes"
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	"github.com/iotexproject/go-pkgs/hash"
	"github.com/iotexproject/iotex-address/address"

	"github.com/iotexproject/iotex-core/v2/action/protocol"
	"github.com/iotexproject/iotex-core/v2/action/protocol/rewarding/rewardingpb"
	"github.com/iotexproject/iotex-core/v2/blockchain/genesis"
	"github.com/iotexproject/iotex-core/v2/state"
	"github.com/iotexproject/iotex-core/v2/systemcontracts"
)

// admin stores the admin data of the rewarding protocol
type admin struct {
	blockReward                    *big.Int
	epochReward                    *big.Int
	numDelegatesForEpochReward     uint64
	foundationBonus                *big.Int
	numDelegatesForFoundationBonus uint64
	foundationBonusLastEpoch       uint64
	productivityThreshold          uint64
}

var _ protocol.ContractStorage = (*admin)(nil)

// Serialize serializes admin state into bytes
func (a admin) Serialize() ([]byte, error) {
	gen := rewardingpb.Admin{
		BlockReward:                    a.blockReward.String(),
		EpochReward:                    a.epochReward.String(),
		NumDelegatesForEpochReward:     a.numDelegatesForEpochReward,
		FoundationBonus:                a.foundationBonus.String(),
		NumDelegatesForFoundationBonus: a.numDelegatesForFoundationBonus,
		FoundationBonusLastEpoch:       a.foundationBonusLastEpoch,
		ProductivityThreshold:          a.productivityThreshold,
	}
	return proto.Marshal(&gen)
}

// Deserialize deserializes bytes into admin state
func (a *admin) Deserialize(data []byte) error {
	gen := rewardingpb.Admin{}
	if err := proto.Unmarshal(data, &gen); err != nil {
		return err
	}
	blockReward, ok := new(big.Int).SetString(gen.BlockReward, 10)
	if !ok {
		return errors.New("failed to set block reward")
	}
	epochReward, ok := new(big.Int).SetString(gen.EpochReward, 10)
	if !ok {
		return errors.New("failed to set epoch reward")
	}
	foundationBonus, ok := new(big.Int).SetString(gen.FoundationBonus, 10)
	if !ok {
		return errors.New("failed to set bootstrap bonus")
	}
	a.blockReward = blockReward
	a.epochReward = epochReward
	a.numDelegatesForEpochReward = gen.NumDelegatesForEpochReward
	a.foundationBonus = foundationBonus
	a.numDelegatesForFoundationBonus = gen.NumDelegatesForFoundationBonus
	a.foundationBonusLastEpoch = gen.FoundationBonusLastEpoch
	a.productivityThreshold = gen.ProductivityThreshold
	return nil
}

func (a *admin) grantFoundationBonus(epoch uint64) bool {
	return epoch <= a.foundationBonusLastEpoch
}

func (a *admin) storageContractAddress(ns string, key []byte) (address.Address, error) {
	prefix := hash.Hash160b([]byte(_protocolID))
	if ns == state.AccountKVNamespace {
		expectKey := hash.Hash160b(append(prefix[:], _adminKey...))
		if !bytes.Equal(expectKey[:], key) {
			return nil, errors.Errorf("unexpected key %x, expected %x", key, expectKey)
		}
		return systemcontracts.SystemContracts[systemcontracts.RewardingContractV1Index].Address, nil
	} else if ns == _v2RewardingNamespace {
		expectKey := append(prefix[:], _adminKey...)
		if !bytes.Equal(expectKey[:], key) {
			return nil, errors.Errorf("unexpected key %x, expected %x", key, expectKey)
		}
		return systemcontracts.SystemContracts[systemcontracts.RewardingContractV2Index].Address, nil
	} else {
		return nil, errors.Errorf("unexpected namespace %s", ns)
	}
}

func (a *admin) storageContract(ns string, key []byte, backend systemcontracts.ContractBackend) (*systemcontracts.GenericStorageContract, error) {
	addr, err := a.storageContractAddress(ns, key)
	if err != nil {
		return nil, err
	}
	contract, err := systemcontracts.NewGenericStorageContract(common.BytesToAddress(addr.Bytes()), backend)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create admin storage contract")
	}
	return contract, nil
}

func (a *admin) StoreToContract(ns string, key []byte, backend systemcontracts.ContractBackend) error {
	contract, err := a.storageContract(ns, key, backend)
	if err != nil {
		return err
	}
	data, err := a.Serialize()
	if err != nil {
		return errors.Wrap(err, "failed to serialize admin state")
	}
	if err := contract.Put(key, systemcontracts.GenericValue{PrimaryData: data}); err != nil {
		return errors.Wrapf(err, "failed to store admin state to contract %s", contract.Address().Hex())
	}
	return nil
}

func (a *admin) LoadFromContract(ns string, key []byte, backend systemcontracts.ContractBackend) error {
	contract, err := a.storageContract(ns, key, backend)
	if err != nil {
		return err
	}
	value, err := contract.Get(key)
	if err != nil {
		return errors.Wrapf(err, "failed to get admin state from contract %s with key %x", contract.Address().Hex(), key)
	}
	if !value.KeyExists {
		return errors.Wrapf(state.ErrStateNotExist, "admin state does not exist in contract %s with key %x", contract.Address().Hex(), key)
	}
	if err := a.Deserialize(value.Value.PrimaryData); err != nil {
		return errors.Wrap(err, "failed to deserialize admin state")
	}
	return nil
}

func (a *admin) DeleteFromContract(ns string, key []byte, backend systemcontracts.ContractBackend) error {
	contract, err := a.storageContract(ns, key, backend)
	if err != nil {
		return err
	}
	if err := contract.Remove(key); err != nil {
		return errors.Wrapf(err, "failed to delete admin state from contract %s with key %x", contract.Address().Hex(), key)
	}
	return nil
}

func (a *admin) ListFromContract(ns string, backend systemcontracts.ContractBackend) ([][]byte, []any, error) {
	return nil, nil, errors.New("not implemented")
}

func (a *admin) BatchFromContract(ns string, keys [][]byte, backend systemcontracts.ContractBackend) ([]any, error) {
	return nil, errors.New("not implemented")
}

// exempt stores the addresses that exempt from epoch reward
type exempt struct {
	addrs []address.Address
}

var _ protocol.ContractStorage = (*exempt)(nil)

// Serialize serializes exempt state into bytes
func (e *exempt) Serialize() ([]byte, error) {
	epb := rewardingpb.Exempt{}
	for _, addr := range e.addrs {
		epb.Addrs = append(epb.Addrs, addr.Bytes())
	}
	return proto.Marshal(&epb)
}

// Deserialize deserializes bytes into exempt state
func (e *exempt) Deserialize(data []byte) error {
	epb := rewardingpb.Exempt{}
	if err := proto.Unmarshal(data, &epb); err != nil {
		return err
	}
	e.addrs = nil
	for _, addrBytes := range epb.Addrs {
		addr, err := address.FromBytes(addrBytes)
		if err != nil {
			return err
		}
		e.addrs = append(e.addrs, addr)
	}
	return nil
}

func (e *exempt) storageContractAddress(ns string, key []byte) (address.Address, error) {
	prefix := hash.Hash160b([]byte(_protocolID))
	if ns == state.AccountKVNamespace {
		expectKey := hash.Hash160b(append(prefix[:], _exemptKey...))
		if !bytes.Equal(expectKey[:], key) {
			return nil, errors.Errorf("unexpected key %x, expected %x", key, expectKey)
		}
		return systemcontracts.SystemContracts[systemcontracts.RewardingContractV1Index].Address, nil
	} else if ns == _v2RewardingNamespace {
		expectKey := append(prefix[:], _exemptKey...)
		if !bytes.Equal(expectKey[:], key) {
			return nil, errors.Errorf("unexpected key %x, expected %x", key, expectKey)
		}
		return systemcontracts.SystemContracts[systemcontracts.RewardingContractV2Index].Address, nil
	} else {
		return nil, errors.Errorf("unexpected namespace %s", ns)
	}
}

func (e *exempt) storageContract(ns string, key []byte, backend systemcontracts.ContractBackend) (*systemcontracts.GenericStorageContract, error) {
	addr, err := e.storageContractAddress(ns, key)
	if err != nil {
		return nil, err
	}
	contract, err := systemcontracts.NewGenericStorageContract(common.BytesToAddress(addr.Bytes()), backend)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create exempt storage contract")
	}
	return contract, nil
}

func (e *exempt) StoreToContract(ns string, key []byte, backend systemcontracts.ContractBackend) error {
	contract, err := e.storageContract(ns, key, backend)
	if err != nil {
		return errors.Wrapf(err, "failed to get exempt storage contract")
	}
	data, err := e.Serialize()
	if err != nil {
		return errors.Wrap(err, "failed to serialize exempt state")
	}
	if err := contract.Put(key, systemcontracts.GenericValue{PrimaryData: data}); err != nil {
		return errors.Wrapf(err, "failed to store exempt state to contract %s", contract.Address().Hex())
	}
	return nil
}

func (e *exempt) LoadFromContract(ns string, key []byte, backend systemcontracts.ContractBackend) error {
	contract, err := e.storageContract(ns, key, backend)
	if err != nil {
		return errors.Wrapf(err, "failed to get exempt storage contract")
	}
	value, err := contract.Get(key)
	if err != nil {
		return errors.Wrapf(err, "failed to get exempt state from contract %s with key %x", contract.Address().Hex(), key)
	}
	if !value.KeyExists {
		return errors.Wrapf(state.ErrStateNotExist, "exempt state does not exist in contract %s with key %x", contract.Address().Hex(), key)
	}
	if err := e.Deserialize(value.Value.PrimaryData); err != nil {
		return errors.Wrap(err, "failed to deserialize exempt state")
	}
	return nil
}

func (e *exempt) DeleteFromContract(ns string, key []byte, backend systemcontracts.ContractBackend) error {
	contract, err := e.storageContract(ns, key, backend)
	if err != nil {
		return errors.Wrapf(err, "failed to get exempt storage contract")
	}
	if err := contract.Remove(key); err != nil {
		return errors.Wrapf(err, "failed to delete exempt state from contract %s with key %x", contract.Address().Hex(), key)
	}
	return nil
}

func (e *exempt) ListFromContract(ns string, backend systemcontracts.ContractBackend) ([][]byte, []any, error) {
	return nil, nil, errors.New("not implemented")
}

func (e *exempt) BatchFromContract(ns string, keys [][]byte, backend systemcontracts.ContractBackend) ([]any, error) {
	return nil, errors.New("not implemented")
}

// CreateGenesisStates initializes the rewarding protocol by setting the original admin, block and epoch reward
func (p *Protocol) CreateGenesisStates(
	ctx context.Context,
	sm protocol.StateManager,
) error {
	blkCtx := protocol.MustGetBlockCtx(ctx)
	g := genesis.MustExtractGenesisContext(ctx)
	if err := p.assertZeroBlockHeight(blkCtx.BlockHeight); err != nil {
		return err
	}

	blockReward := g.BlockReward()
	if err := p.assertAmount(blockReward); err != nil {
		return err
	}

	epochReward := g.EpochReward()
	if err := p.assertAmount(epochReward); err != nil {
		return err
	}

	if err := p.putState(
		ctx,
		sm,
		_adminKey,
		&admin{
			blockReward:                    blockReward,
			epochReward:                    epochReward,
			numDelegatesForEpochReward:     g.NumDelegatesForEpochReward,
			foundationBonus:                g.FoundationBonus(),
			numDelegatesForFoundationBonus: g.NumDelegatesForFoundationBonus,
			foundationBonusLastEpoch:       g.FoundationBonusLastEpoch,
			productivityThreshold:          g.ProductivityThreshold,
		},
	); err != nil {
		return err
	}

	initBalance := g.InitBalance()
	if err := p.putState(
		ctx,
		sm,
		_fundKey,
		&fund{
			totalBalance:     initBalance,
			unclaimedBalance: initBalance,
		},
	); err != nil {
		return err
	}
	return p.putState(
		ctx,
		sm,
		_exemptKey,
		&exempt{
			addrs: g.ExemptAddrsFromEpochReward(),
		},
	)
}

// BlockReward returns the block reward amount
func (p *Protocol) BlockReward(
	ctx context.Context,
	sm protocol.StateReader,
) (*big.Int, error) {
	a := admin{}
	if _, err := p.state(ctx, sm, _adminKey, &a); err != nil {
		return nil, err
	}
	return a.blockReward, nil
}

// EpochReward returns the epoch reward amount
func (p *Protocol) EpochReward(
	ctx context.Context,
	sm protocol.StateReader,
) (*big.Int, error) {
	a := admin{}
	if _, err := p.state(ctx, sm, _adminKey, &a); err != nil {
		return nil, err
	}
	return a.epochReward, nil
}

// NumDelegatesForEpochReward returns the number of candidates sharing an epoch reward
func (p *Protocol) NumDelegatesForEpochReward(
	ctx context.Context,
	sm protocol.StateManager,
) (uint64, error) {
	a := admin{}
	if _, err := p.state(ctx, sm, _adminKey, &a); err != nil {
		return 0, err
	}
	return a.numDelegatesForEpochReward, nil
}

// FoundationBonus returns the foundation bonus amount
func (p *Protocol) FoundationBonus(ctx context.Context, sm protocol.StateReader) (*big.Int, error) {
	a := admin{}
	if _, err := p.state(ctx, sm, _adminKey, &a); err != nil {
		return nil, err
	}
	return a.foundationBonus, nil
}

// FoundationBonusLastEpoch returns the last epoch when the foundation bonus will still be granted
func (p *Protocol) FoundationBonusLastEpoch(ctx context.Context, sm protocol.StateReader) (uint64, error) {
	a := admin{}
	if _, err := p.state(ctx, sm, _adminKey, &a); err != nil {
		return 0, err
	}
	return a.foundationBonusLastEpoch, nil
}

// NumDelegatesForFoundationBonus returns the number of delegates that will get foundation bonus
func (p *Protocol) NumDelegatesForFoundationBonus(ctx context.Context, sm protocol.StateReader) (uint64, error) {
	a := admin{}
	if _, err := p.state(ctx, sm, _adminKey, &a); err != nil {
		return 0, err
	}
	return a.numDelegatesForFoundationBonus, nil
}

// ProductivityThreshold returns the productivity threshold
func (p *Protocol) ProductivityThreshold(ctx context.Context, sm protocol.StateManager) (uint64, error) {
	a := admin{}
	if _, err := p.state(ctx, sm, _adminKey, &a); err != nil {
		return 0, err
	}
	return a.productivityThreshold, nil
}

// SetReward updates block or epoch reward amount
func (p *Protocol) SetReward(
	ctx context.Context,
	sm protocol.StateManager,
	amount *big.Int,
	blockLevel bool,
) error {
	if err := p.assertAmount(amount); err != nil {
		return err
	}
	a := admin{}
	if _, err := p.state(ctx, sm, _adminKey, &a); err != nil {
		return err
	}
	if blockLevel {
		a.blockReward = amount
	} else {
		a.epochReward = amount
	}
	return p.putState(ctx, sm, _adminKey, &a)
}

func (p *Protocol) assertAmount(amount *big.Int) error {
	if amount.Cmp(big.NewInt(0)) >= 0 {
		return nil
	}
	return errors.Errorf("amount %s shouldn't be negative", amount.String())
}

func (p *Protocol) assertZeroBlockHeight(height uint64) error {
	if height != 0 {
		return errors.Errorf("current block height %d is not zero", height)
	}
	return nil
}
