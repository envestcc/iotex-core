package evm

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	types2 "github.com/ledgerwatch/erigon-lib/types"
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
			precompiles []libcommon.Address, list types2.AccessList)
		GetCode(addr libcommon.Address) []byte
		GetCodeSize(addr libcommon.Address) int
		GetCodeHash(addr libcommon.Address) libcommon.Hash
		GetCommittedState(addr libcommon.Address, key *libcommon.Hash, value *uint256.Int)
		GetState(addr libcommon.Address, key *libcommon.Hash, value *uint256.Int)
		SetState(addr libcommon.Address, key *libcommon.Hash, value uint256.Int)
		SetCode(addr libcommon.Address, code []byte)
		GetNonce(addr libcommon.Address) uint64
		GetBalance(addr libcommon.Address) *uint256.Int
		SetNonce(addr libcommon.Address, nonce uint64)
	}
)
