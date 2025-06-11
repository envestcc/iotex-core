package factory

import (
	"context"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	types2 "github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types/accounts"
	"github.com/holiman/uint256"
	"go.uber.org/zap"

	"github.com/iotexproject/iotex-core/v2/pkg/log"
)

type (
	ErigonIntraBlockState interface {
		CreateAccount(addr libcommon.Address, contractCreation bool)
		AddRefund(gas uint64)
		SubRefund(gas uint64)
		Selfdestruct(addr libcommon.Address) bool
		Selfdestruct6780(addr libcommon.Address)
		AddAddressToAccessList(addr libcommon.Address) (addrMod bool)
		AddSlotToAccessList(addr libcommon.Address, slot libcommon.Hash) (addrMod, slotMod bool)
		Prepare(rules *chain.Rules, sender, coinbase libcommon.Address, dst *libcommon.Address,
			precompiles []libcommon.Address, list types2.AccessList, authorities []libcommon.Address)
		GetCode(addr libcommon.Address) []byte
		GetCodeSize(addr libcommon.Address) int
		GetCodeHash(addr libcommon.Address) libcommon.Hash
		GetCommittedState(addr libcommon.Address, key *libcommon.Hash, value *uint256.Int)
		GetState(addr libcommon.Address, key *libcommon.Hash, value *uint256.Int)
		SetState(addr libcommon.Address, key *libcommon.Hash, value uint256.Int)
		SetCode(addr libcommon.Address, code []byte)
		GetNonce(addr libcommon.Address) uint64
		GetBalance(addr libcommon.Address) *uint256.Int

		FinalizeTx(chainRules *chain.Rules, stateWriter state.StateWriter) error
		CommitBlock(chainRules *chain.Rules, stateWriter state.StateWriter) error
		Snapshot() int
		RevertToSnapshot(revid int)
		Exist(addr libcommon.Address) bool
		SetBalance(addr libcommon.Address, amount *uint256.Int)
		SetNonce(addr libcommon.Address, nonce uint64)
	}

	RWDB interface {
		Close()
		BeginRw(ctx context.Context) (kv.RwTx, error)
		BeginRo(ctx context.Context) (kv.Tx, error)
	}

	rwdbW struct {
		kv.RwDB
		l *zap.Logger
	}

	rwtxW struct {
		kv.RwTx
		l *zap.Logger
	}

	rotxW struct {
		kv.Tx
		l *zap.Logger
	}

	intraBlockStateW struct {
		e ErigonIntraBlockState
		l *zap.Logger
	}

	swcw struct {
		w state.WriterWithChangeSets
		l *zap.Logger
	}

	sww struct {
		w state.StateWriter
		l *zap.Logger
	}
)

func NewRWDBW(rw kv.RwDB) RWDB {
	return &rwdbW{
		RwDB: rw,
		l:    log.L().With(zap.String("topic", "erigon"), zap.String("comp", "db")),
	}
}

func (db *rwdbW) Close() {
	db.l.Debug("closing erigon db")
	db.RwDB.Close()
}

func (db *rwdbW) BeginRw(ctx context.Context) (kv.RwTx, error) {
	db.l.Debug("beginning rw transaction")
	return db.RwDB.BeginRw(ctx)
}

func (db *rwdbW) BeginRo(ctx context.Context) (kv.Tx, error) {
	db.l.Debug("beginning ro transaction")
	return db.RwDB.BeginRo(ctx)
}

func NewRWTxW(tx kv.RwTx) kv.RwTx {
	return &rwtxW{
		RwTx: tx,
		l:    log.L().With(zap.String("topic", "erigon"), zap.String("comp", "rwtx"), zap.Int64("uuid", time.Now().UnixNano())).WithOptions(zap.AddCaller(), zap.AddCallerSkip(1)),
	}
}

func (tx *rwtxW) Commit() error {
	tx.l.Debug("committing transaction")
	return tx.RwTx.Commit()
}

func (tx *rwtxW) Rollback() {
	tx.l.Debug("rolling back transaction")
	tx.RwTx.Rollback()
}

func (tx *rwtxW) Put(table string, key, value []byte) error {
	tx.l.Debug("putting key-value pair", zap.ByteString("key", key), zap.ByteString("value", value))
	return tx.RwTx.Put(table, key, value)
}

func NewRoTxW(tx kv.Tx) kv.Tx {
	return &rotxW{
		Tx: tx,
		l:  log.L().With(zap.String("topic", "erigon"), zap.String("comp", "rotx")),
	}
}

func (tx *rotxW) Commit() error {
	tx.l.Debug("committing transaction")
	return tx.Tx.Commit()
}

func (tx *rotxW) Rollback() {
	tx.l.Debug("rolling back transaction")
	tx.Tx.Rollback()
}

func NewIntraBlockStateW(ibs ErigonIntraBlockState) ErigonIntraBlockState {
	return &intraBlockStateW{
		e: ibs,
		l: log.L().With(zap.String("topic", "erigon"), zap.String("comp", "intrablockstate")),
	}
}

func (ibs *intraBlockStateW) CreateAccount(addr libcommon.Address, contractCreation bool) {
	ibs.l.Debug("creating account", zap.String("address", addr.Hex()), zap.Bool("contractCreation", contractCreation))
	ibs.e.CreateAccount(addr, contractCreation)
}

func (ibs *intraBlockStateW) AddRefund(gas uint64) {
	ibs.l.Debug("adding refund", zap.Uint64("gas", gas))
	ibs.e.AddRefund(gas)
}

func (ibs *intraBlockStateW) SubRefund(gas uint64) {
	ibs.l.Debug("subtracting refund", zap.Uint64("gas", gas))
	ibs.e.SubRefund(gas)
}

func (ibs *intraBlockStateW) Selfdestruct(addr libcommon.Address) bool {
	ibs.l.Debug("self-destructing account", zap.String("address", addr.Hex()))
	return ibs.e.Selfdestruct(addr)
}

func (ibs *intraBlockStateW) Selfdestruct6780(addr libcommon.Address) {
	ibs.l.Debug("self-destructing account (EIP-6780)", zap.String("address", addr.Hex()))
	ibs.e.Selfdestruct6780(addr)
}

func (ibs *intraBlockStateW) AddAddressToAccessList(addr libcommon.Address) (addrMod bool) {
	ibs.l.Debug("adding address to access list", zap.String("address", addr.Hex()))
	addrMod = ibs.e.AddAddressToAccessList(addr)
	return addrMod
}

func (ibs *intraBlockStateW) AddSlotToAccessList(addr libcommon.Address, slot libcommon.Hash) (addrMod, slotMod bool) {
	ibs.l.Debug("adding slot to access list", zap.String("address", addr.Hex()), zap.String("slot", slot.Hex()))
	addrMod, slotMod = ibs.e.AddSlotToAccessList(addr, slot)
	return addrMod, slotMod
}

func (ibs *intraBlockStateW) Prepare(rules *chain.Rules, sender, coinbase libcommon.Address, dst *libcommon.Address,
	precompiles []libcommon.Address, list types2.AccessList, authorities []libcommon.Address) {
	ibs.l.Debug("preparing intra-block state",
		zap.String("sender", sender.Hex()),
		zap.String("coinbase", coinbase.Hex()),
		zap.String("destination", dst.Hex()),
	)
	ibs.e.Prepare(rules, sender, coinbase, dst, precompiles, list, authorities)
}

func (ibs *intraBlockStateW) GetCode(addr libcommon.Address) []byte {
	ibs.l.Debug("getting code for address", zap.String("address", addr.Hex()))
	return ibs.e.GetCode(addr)
}
func (ibs *intraBlockStateW) GetCodeSize(addr libcommon.Address) int {
	ibs.l.Debug("getting code size for address", zap.String("address", addr.Hex()))
	return ibs.e.GetCodeSize(addr)
}
func (ibs *intraBlockStateW) GetCodeHash(addr libcommon.Address) libcommon.Hash {
	ibs.l.Debug("getting code hash for address", zap.String("address", addr.Hex()))
	return ibs.e.GetCodeHash(addr)
}
func (ibs *intraBlockStateW) GetCommittedState(addr libcommon.Address, key *libcommon.Hash, value *uint256.Int) {
	ibs.l.Debug("getting committed state",
		zap.String("address", addr.Hex()),
		zap.String("key", key.Hex()),
	)
	ibs.e.GetCommittedState(addr, key, value)
}
func (ibs *intraBlockStateW) GetState(addr libcommon.Address, key *libcommon.Hash, value *uint256.Int) {
	ibs.l.Debug("getting state",
		zap.String("address", addr.Hex()),
		zap.String("key", key.Hex()),
	)
	ibs.e.GetState(addr, key, value)
}
func (ibs *intraBlockStateW) SetState(addr libcommon.Address, key *libcommon.Hash, value uint256.Int) {
	ibs.l.Debug("setting state",
		zap.String("address", addr.Hex()),
		zap.String("key", key.Hex()),
		zap.String("value", value.String()),
	)
	ibs.e.SetState(addr, key, value)
}
func (ibs *intraBlockStateW) SetCode(addr libcommon.Address, code []byte) {
	ibs.l.Debug("setting code for address", zap.String("address", addr.Hex()), zap.Int("codeSize", len(code)))
	ibs.e.SetCode(addr, code)
}
func (ibs *intraBlockStateW) GetNonce(addr libcommon.Address) uint64 {
	ibs.l.Debug("getting nonce for address", zap.String("address", addr.Hex()))
	return ibs.e.GetNonce(addr)
}
func (ibs *intraBlockStateW) GetBalance(addr libcommon.Address) *uint256.Int {
	ibs.l.Debug("getting balance for address", zap.String("address", addr.Hex()))
	return ibs.e.GetBalance(addr)
}
func (ibs *intraBlockStateW) FinalizeTx(chainRules *chain.Rules, stateWriter state.StateWriter) error {
	ibs.l.Debug("finalizing transaction")
	return ibs.e.FinalizeTx(chainRules, stateWriter)
}
func (ibs *intraBlockStateW) CommitBlock(chainRules *chain.Rules, stateWriter state.StateWriter) error {
	ibs.l.Debug("committing block")
	return ibs.e.CommitBlock(chainRules, stateWriter)
}
func (ibs *intraBlockStateW) Snapshot() int {
	ibs.l.Debug("taking snapshot")
	return ibs.e.Snapshot()
}
func (ibs *intraBlockStateW) RevertToSnapshot(revid int) {
	ibs.l.Debug("reverting to snapshot", zap.Int("snapshotID", revid))
	ibs.e.RevertToSnapshot(revid)
}
func (ibs *intraBlockStateW) Exist(addr libcommon.Address) bool {
	ibs.l.Debug("checking existence of address", zap.String("address", addr.Hex()))
	return ibs.e.Exist(addr)
}
func (ibs *intraBlockStateW) SetBalance(addr libcommon.Address, amount *uint256.Int) {
	ibs.l.Debug("setting balance for address", zap.String("address", addr.Hex()), zap.String("amount", amount.String()))
	ibs.e.SetBalance(addr, amount)
}
func (ibs *intraBlockStateW) SetNonce(addr libcommon.Address, nonce uint64) {
	ibs.l.Debug("setting nonce for address", zap.String("address", addr.Hex()), zap.Uint64("nonce", nonce))
	ibs.e.SetNonce(addr, nonce)
}

func NewStateWriterWithChangeSetsW(w state.WriterWithChangeSets) state.WriterWithChangeSets {
	return &swcw{
		w: w,
		l: log.L().With(zap.String("topic", "erigon"), zap.String("comp", "WriterWithChangeSets")),
	}
}

func (w *swcw) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	w.l.Debug("updating account data",
		zap.String("address", address.Hex()),
	)
	return w.w.UpdateAccountData(address, original, account)
}
func (w *swcw) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	w.l.Debug("updating account code",
		zap.String("address", address.Hex()),
		zap.Uint64("incarnation", incarnation),
		zap.String("codeHash", codeHash.Hex()),
		zap.Int("codeSize", len(code)),
	)
	return w.w.UpdateAccountCode(address, incarnation, codeHash, code)
}
func (w *swcw) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	w.l.Debug("deleting account",
		zap.String("address", address.Hex()),
	)
	return w.w.DeleteAccount(address, original)
}
func (w *swcw) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	w.l.Debug("writing account storage",
		zap.String("address", address.Hex()),
		zap.Uint64("incarnation", incarnation),
		zap.String("key", key.Hex()),
		zap.String("original", original.String()),
		zap.String("value", value.String()),
	)
	return w.w.WriteAccountStorage(address, incarnation, key, original, value)
}

func (w *swcw) CreateContract(address libcommon.Address) error {
	w.l.Debug("creating contract",
		zap.String("address", address.Hex()),
	)
	return w.w.CreateContract(address)
}

func (w *swcw) WriteChangeSets() error {
	w.l.Debug("writing change sets")
	return w.w.WriteChangeSets()
}
func (w *swcw) WriteHistory() error {
	w.l.Debug("writing history")
	return w.w.WriteHistory()
}

func NewStateWriterW(w state.StateWriter) state.StateWriter {
	return &sww{
		w: w,
		l: log.L().With(zap.String("topic", "erigon"), zap.String("comp", "stateWriter")),
	}
}

func (w *sww) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	w.l.Debug("updating account data",
		zap.String("address", address.Hex()),
	)
	return w.w.UpdateAccountData(address, original, account)
}
func (w *sww) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	w.l.Debug("updating account code",
		zap.String("address", address.Hex()),
		zap.Uint64("incarnation", incarnation),
		zap.String("codeHash", codeHash.Hex()),
		zap.Int("codeSize", len(code)),
	)
	return w.w.UpdateAccountCode(address, incarnation, codeHash, code)
}
func (w *sww) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	w.l.Debug("deleting account",
		zap.String("address", address.Hex()),
	)
	return w.w.DeleteAccount(address, original)
}
func (w *sww) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	w.l.Debug("writing account storage",
		zap.String("address", address.Hex()),
		zap.Uint64("incarnation", incarnation),
		zap.String("key", key.Hex()),
		zap.String("original", original.String()),
		zap.String("value", value.String()),
	)
	return w.w.WriteAccountStorage(address, incarnation, key, original, value)
}

func (w *sww) CreateContract(address libcommon.Address) error {
	w.l.Debug("creating contract",
		zap.String("address", address.Hex()),
	)
	return w.w.CreateContract(address)
}
