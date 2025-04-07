package evm

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
	"go.uber.org/zap"

	"github.com/iotexproject/iotex-core/v2/action"
	"github.com/iotexproject/iotex-core/v2/pkg/log"
)

type adapterWithLog struct {
	adapter    StateDB
	logCounter int
}

var _ StateDB = (*adapterWithLog)(nil)

func newAdapterWithLog(adapter StateDB) *adapterWithLog {
	return &adapterWithLog{
		adapter: adapter,
	}
}

func (adt *adapterWithLog) log(msg string, fields ...zap.Field) {
	fields = append(fields, zap.Int("logCounter", adt.logCounter), zap.String("adapter", "adapterWithLog"))
	log.L().Info(msg, fields...)
	adt.logCounter++
}

func (adt *adapterWithLog) CreateAccount(addr common.Address) {
	adt.log("CreateAccount", zap.String("address", addr.Hex()))
	adt.adapter.CreateAccount(addr)
}

func (adt *adapterWithLog) SubBalance(addr common.Address, balance *uint256.Int) {
	adt.log("SubBalance", zap.String("address", addr.Hex()), zap.String("balance", balance.String()))
	adt.adapter.SubBalance(addr, balance)
}
func (adt *adapterWithLog) AddBalance(addr common.Address, balance *uint256.Int) {
	adt.log("AddBalance", zap.String("address", addr.Hex()), zap.String("balance", balance.String()))
	adt.adapter.AddBalance(addr, balance)
}
func (adt *adapterWithLog) GetBalance(addr common.Address) (ret *uint256.Int) {
	ret = adt.adapter.GetBalance(addr)
	adt.log("GetBalance", zap.String("address", addr.Hex()), zap.String("balance", ret.String()))
	return
}

func (adt *adapterWithLog) IsNewAccount(addr common.Address) (ret bool) {
	ret = adt.adapter.IsNewAccount(addr)
	adt.log("IsNewAccount", zap.String("address", addr.Hex()), zap.Bool("isNewAccount", ret))
	return
}
func (adt *adapterWithLog) GetNonce(addr common.Address) (ret uint64) {
	ret = adt.adapter.GetNonce(addr)
	adt.log("GetNonce", zap.String("address", addr.Hex()), zap.Uint64("nonce", ret))
	return
}
func (adt *adapterWithLog) SetNonce(addr common.Address, nonce uint64) {
	adt.log("SetNonce", zap.String("address", addr.Hex()), zap.Uint64("nonce", nonce))
	adt.adapter.SetNonce(addr, nonce)
}

func (adt *adapterWithLog) GetCodeHash(addr common.Address) (ret common.Hash) {
	ret = adt.adapter.GetCodeHash(addr)
	adt.log("GetCodeHash", zap.String("address", addr.Hex()), zap.String("codeHash", ret.Hex()))
	return
}
func (adt *adapterWithLog) GetCode(addr common.Address) (ret []byte) {
	ret = adt.adapter.GetCode(addr)
	adt.log("GetCode", zap.String("address", addr.Hex()), log.Hex("code", ret))
	return
}
func (adt *adapterWithLog) SetCode(addr common.Address, code []byte) {
	adt.log("SetCode", zap.String("address", addr.Hex()), log.Hex("code", code))
	adt.adapter.SetCode(addr, code)
}
func (adt *adapterWithLog) GetCodeSize(addr common.Address) (ret int) {
	ret = adt.adapter.GetCodeSize(addr)
	adt.log("GetCodeSize", zap.String("address", addr.Hex()), zap.Int("codeSize", ret))
	return
}

func (adt *adapterWithLog) AddRefund(r uint64) {
	adt.log("AddRefund", zap.Uint64("refund", r))
	adt.adapter.AddRefund(r)
}
func (adt *adapterWithLog) SubRefund(r uint64) {
	adt.log("SubRefund", zap.Uint64("refund", r))
	adt.adapter.SubRefund(r)
}
func (adt *adapterWithLog) GetRefund() (ret uint64) {
	ret = adt.adapter.GetRefund()
	adt.log("GetRefund", zap.Uint64("refund", ret))
	return
}
func (adt *adapterWithLog) GetCommittedState(addr common.Address, h common.Hash) (ret common.Hash) {
	ret = adt.adapter.GetCommittedState(addr, h)
	adt.log("GetCommittedState", zap.String("address", addr.Hex()), zap.String("hash", h.Hex()), zap.String("value", ret.Hex()))
	return
}
func (adt *adapterWithLog) GetState(addr common.Address, h common.Hash) (ret common.Hash) {
	ret = adt.adapter.GetState(addr, h)
	adt.log("GetState", zap.String("address", addr.Hex()), zap.String("hash", h.Hex()), zap.String("value", ret.Hex()))
	return
}
func (adt *adapterWithLog) SetState(addr common.Address, k common.Hash, v common.Hash) {
	adt.log("SetState", zap.String("address", addr.Hex()), zap.String("key", k.Hex()), zap.String("value", v.Hex()))
	adt.adapter.SetState(addr, k, v)
}
func (adt *adapterWithLog) GetTransientState(addr common.Address, key common.Hash) common.Hash {
	ret := adt.adapter.GetTransientState(addr, key)
	adt.log("GetTransientState", zap.String("address", addr.Hex()), zap.String("key", key.Hex()), zap.String("value", ret.Hex()))
	return ret
}
func (adt *adapterWithLog) SetTransientState(addr common.Address, key, value common.Hash) {
	adt.log("SetTransientState", zap.String("address", addr.Hex()), zap.String("key", key.Hex()), zap.String("value", value.Hex()))
	adt.adapter.SetTransientState(addr, key, value)
}
func (adt *adapterWithLog) SelfDestruct(addr common.Address) {
	adt.log("SelfDestruct", zap.String("address", addr.Hex()))
	adt.adapter.SelfDestruct(addr)
}
func (adt *adapterWithLog) HasSelfDestructed(addr common.Address) (ret bool) {
	ret = adt.adapter.HasSelfDestructed(addr)
	adt.log("HasSelfDestructed", zap.String("address", addr.Hex()), zap.Bool("hasSelfDestructed", ret))
	return
}
func (adt *adapterWithLog) Selfdestruct6780(addr common.Address) {
	adt.log("Selfdestruct6780", zap.String("address", addr.Hex()))
	adt.adapter.Selfdestruct6780(addr)
}
func (adt *adapterWithLog) Exist(addr common.Address) (ret bool) {
	ret = adt.adapter.Exist(addr)
	adt.log("Exist", zap.String("address", addr.Hex()), zap.Bool("exist", ret))
	return
}
func (adt *adapterWithLog) Empty(addr common.Address) (ret bool) {
	ret = adt.adapter.Empty(addr)
	adt.log("Empty", zap.String("address", addr.Hex()), zap.Bool("empty", ret))
	return
}
func (adt *adapterWithLog) AddressInAccessList(addr common.Address) (ret bool) {
	ret = adt.adapter.AddressInAccessList(addr)
	adt.log("AddressInAccessList", zap.String("address", addr.Hex()), zap.Bool("inAccessList", ret))
	return
}
func (adt *adapterWithLog) SlotInAccessList(addr common.Address, slot common.Hash) (addressOk bool, slotOk bool) {
	addressOk, slotOk = adt.adapter.SlotInAccessList(addr, slot)
	adt.log("SlotInAccessList", zap.String("address", addr.Hex()), zap.String("slot", slot.Hex()), zap.Bool("addressOk", addressOk), zap.Bool("slotOk", slotOk))
	return
}
func (adt *adapterWithLog) AddAddressToAccessList(addr common.Address) {
	adt.log("AddAddressToAccessList", zap.String("address", addr.Hex()))
	adt.adapter.AddAddressToAccessList(addr)
}
func (adt *adapterWithLog) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	adt.log("AddSlotToAccessList", zap.String("address", addr.Hex()), zap.String("slot", slot.Hex()))
	adt.adapter.AddSlotToAccessList(addr, slot)
}
func (adt *adapterWithLog) Prepare(rules params.Rules, sender, coinbase common.Address, dest *common.Address, precompiles []common.Address, txAccesses types.AccessList) {
	adt.log("Prepare", zap.String("sender", sender.Hex()), zap.String("coinbase", coinbase.Hex()), zap.String("dest", dest.Hex()))
	adt.adapter.Prepare(rules, sender, coinbase, dest, precompiles, txAccesses)
}
func (adt *adapterWithLog) RevertToSnapshot(sn int) {
	adt.log("RevertToSnapshot", zap.Int("sn", sn))
	adt.adapter.RevertToSnapshot(sn)
}
func (adt *adapterWithLog) Snapshot() (ret int) {
	ret = adt.adapter.Snapshot()
	adt.log("Snapshot", zap.Int("sn", ret))
	return
}

func (adt *adapterWithLog) AddLog(l *types.Log) {
	adt.log("AddLog")
	adt.adapter.AddLog(l)
}
func (adt *adapterWithLog) AddPreimage(k common.Hash, v []byte) {
	adt.log("AddPreimage", zap.String("key", k.Hex()), zap.String("value", string(v)))
	adt.adapter.AddPreimage(k, v)
}

func (adt *adapterWithLog) CommitContracts() error {
	adt.log("CommitContracts")
	return adt.adapter.CommitContracts()
}
func (adt *adapterWithLog) Logs() []*action.Log {
	adt.log("Logs")
	return adt.adapter.Logs()
}
func (adt *adapterWithLog) TransactionLogs() []*action.TransactionLog {
	adt.log("TransactionLogs")
	return adt.adapter.TransactionLogs()
}
func (adt *adapterWithLog) clear() {
	adt.log("Clear")
	adt.adapter.clear()
}
func (adt *adapterWithLog) Error() error {
	adt.log("Error")
	return adt.adapter.Error()
}
