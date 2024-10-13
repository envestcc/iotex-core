// Copyright (c) 2019 IoTeX Foundation
// This is an alpha (internal) release and is not suitable for production. This source code is provided 'as is' and no
// warranties are given as to title or non-infringement, merchantability or fitness for purpose and, to the extent
// permitted by law, all liability for your use of the code is disclaimed. This source code is governed by Apache
// License 2.0 that can be found in the LICENSE file.

package util

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"math/big"
	"math/rand"
	"sync"

	"github.com/cenkalti/backoff"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto/kzg4844"
	"github.com/holiman/uint256"
	"github.com/iotexproject/iotex-proto/golang/iotexapi"
	"go.uber.org/zap"

	"github.com/iotexproject/iotex-core/action"
	"github.com/iotexproject/iotex-core/pkg/log"
	"github.com/iotexproject/iotex-core/pkg/unit"
	"github.com/iotexproject/iotex-core/pkg/util/assertions"
)

type nounceManager struct {
	mu           sync.RWMutex
	pendingNonce map[string]uint64
}

// AccountManager is tbd
type AccountManager struct {
	AccountList []*AddressKey
	nonceMng    nounceManager
}

// NewAccountManager is tbd
func NewAccountManager(accounts []*AddressKey) *AccountManager {
	return &AccountManager{
		AccountList: accounts,
		nonceMng: nounceManager{
			pendingNonce: make(map[string]uint64),
		},
	}
}

// Get is tbd
func (ac *AccountManager) Get(addr string) uint64 {
	ac.nonceMng.mu.RLock()
	defer ac.nonceMng.mu.RUnlock()
	return ac.nonceMng.pendingNonce[addr]
}

// GetAndInc is tbd
func (ac *AccountManager) GetAndInc(addr string) uint64 {
	var ret uint64
	ac.nonceMng.mu.Lock()
	defer ac.nonceMng.mu.Unlock()
	ret = ac.nonceMng.pendingNonce[addr]
	ac.nonceMng.pendingNonce[addr]++
	return ret
}

// GetAllAddr is tbd
func (ac *AccountManager) GetAllAddr() []string {
	var ret []string
	for _, v := range ac.AccountList {
		ret = append(ret, v.EncodedAddr)
	}
	return ret
}

// Set is tbd
func (ac *AccountManager) Set(addr string, val uint64) {
	ac.nonceMng.mu.Lock()
	defer ac.nonceMng.mu.Unlock()
	ac.nonceMng.pendingNonce[addr] = val
}

// UpdateNonce is tbd
func (ac *AccountManager) UpdateNonce(client iotexapi.APIServiceClient) error {
	// load the nonce and balance of addr
	for _, account := range ac.AccountList {
		addr := account.EncodedAddr
		err := backoff.Retry(func() error {
			acctDetails, err := client.GetAccount(context.Background(), &iotexapi.GetAccountRequest{Address: addr})
			if err != nil {
				return err
			}
			ac.Set(addr, acctDetails.GetAccountMeta().PendingNonce)
			return nil
		}, backoff.NewExponentialBackOff())
		if err != nil {
			log.L().Fatal("Failed to inject actions by APS",
				zap.Error(err),
				zap.String("addr", account.EncodedAddr))
			return err
		}
	}
	return nil
}

// ActionGenerator is tbd
func ActionGenerator(
	actionType int,
	txType int,
	accountManager *AccountManager,
	chainID uint32,
	evmChainID uint32,
	transferGasLimit uint64,
	transferGasPrice *big.Int,
	executionGasLimit uint64,
	executionGasPrice *big.Int,
	contractAddr string,
	transferPayload, executionPayload []byte,
) (*action.SealedEnvelope, error) {
	var (
		selp      *action.SealedEnvelope
		err       error
		delegates = accountManager.AccountList
		randNum   = rand.Intn(len(delegates))
		sender    = delegates[randNum]
		recipient = delegates[(randNum+1)%len(delegates)]
		nonce     = accountManager.GetAndInc(sender.EncodedAddr)
	)
	switch actionType {
	case 1:
		var commonTx action.TxCommonInternal
		switch txType {
		default:
			commonTx = action.NewLegacyTx(chainID, nonce, transferGasLimit, transferGasPrice)
		case action.BlobTxType:
			sidecar := generateSidecar(1)
			commonTx = action.NewBlobTx(chainID, nonce, transferGasLimit, big.NewInt(1), transferGasPrice, nil, action.NewBlobTxData(uint256.NewInt(1), sidecar.BlobHashes(), sidecar))
		}
		// selp, err = action.Sign(action.NewEnvelope(commonTx, action.NewTransfer(big.NewInt(0), recipient.EncodedAddr, transferPayload)), sender.PriKey)
		selp, err = action.Sign155(action.NewEnvelope(commonTx, action.NewTransfer(big.NewInt(0), recipient.EncodedAddr, transferPayload)), evmChainID, sender.PriKey)
	case 2:
		var commonTx action.TxCommonInternal
		switch txType {
		default:
			commonTx = action.NewLegacyTx(chainID, nonce, transferGasLimit, transferGasPrice)
		case action.BlobTxType:
			sidecar := generateSidecar(1)
			commonTx = action.NewBlobTx(chainID, nonce, transferGasLimit, big.NewInt(1), transferGasPrice, nil, action.NewBlobTxData(uint256.NewInt(1), sidecar.BlobHashes(), sidecar))
		}
		// selp, err = action.Sign(action.NewEnvelope(commonTx, action.NewExecution(contractAddr, big.NewInt(0), executionPayload)), sender.PriKey)
		selp, err = action.Sign155(action.NewEnvelope(commonTx, action.NewExecution(contractAddr, big.NewInt(0), executionPayload)), evmChainID, sender.PriKey)
	case 3:
		if rand.Intn(2) == 0 {
			selp, _, err = createSignedTransfer(sender, recipient, big.NewInt(0), chainID, nonce, transferGasLimit, transferGasPrice, hex.EncodeToString(transferPayload))
		} else {
			selp, _, err = createSignedExecution(sender, contractAddr, chainID, nonce, big.NewInt(0), executionGasLimit, executionGasPrice, hex.EncodeToString(executionPayload))
		}
	}
	return selp, err
}

func ActionGeneratorWeb3(
	actionType int,
	txType int,
	accountManager *AccountManager,
	chainID uint32,
	evmChainID uint32,
	transferGasLimit uint64,
	transferGasPrice *big.Int,
	executionGasLimit uint64,
	executionGasPrice *big.Int,
	contractAddr string,
	transferPayload, executionPayload []byte,
) (*types.Transaction, *AddressKey, error) {
	var (
		err       error
		delegates = accountManager.AccountList
		randNum   = rand.Intn(len(delegates))
		sender    = delegates[randNum]
		recipient = delegates[(randNum+1)%len(delegates)]
		nonce     = accountManager.GetAndInc(sender.EncodedAddr)
		to        = common.BytesToAddress(recipient.PriKey.PublicKey().Address().Bytes())
		tx        *types.Transaction
	)
	switch actionType {
	default:
		switch txType {
		default:
			tx = types.NewTx(&types.LegacyTx{
				Nonce:    nonce,
				GasPrice: transferGasPrice,
				Gas:      transferGasLimit,
				To:       &to,
				Value:    big.NewInt(0),
				Data:     transferPayload,
			})
			tx, err = types.SignTx(tx, types.NewCancunSigner(big.NewInt(int64(evmChainID))), sender.PriKey.EcdsaPrivateKey().(*ecdsa.PrivateKey))
		case action.DynamicFeeTxType:
			tx = types.NewTx(&types.DynamicFeeTx{
				ChainID:   big.NewInt(int64(evmChainID)),
				Nonce:     nonce,
				GasTipCap: big.NewInt(int64(unit.Qev / 2)),
				GasFeeCap: big.NewInt(int64(unit.Qev * 2)),
				Gas:       transferGasLimit,
				To:        &to,
				Value:     big.NewInt(0),
				Data:      transferPayload,
			})
			tx, err = types.SignTx(tx, types.NewCancunSigner(big.NewInt(int64(evmChainID))), sender.PriKey.EcdsaPrivateKey().(*ecdsa.PrivateKey))
		case action.BlobTxType:
			sidecar := generateSidecar(1)
			tx = types.NewTx(&types.BlobTx{
				ChainID:    uint256.NewInt(uint64(evmChainID)),
				Nonce:      uint64(nonce),
				GasTipCap:  uint256.NewInt(uint64(unit.Qev / 2)),
				GasFeeCap:  uint256.NewInt(uint64(unit.Qev * 2)),
				Gas:        uint64(transferGasLimit),
				To:         to,
				Value:      uint256.NewInt(0),
				Data:       transferPayload,
				BlobFeeCap: uint256.NewInt(1),
				BlobHashes: sidecar.BlobHashes(),
				Sidecar:    sidecar,
			})
			tx, err = types.SignTx(tx, types.NewCancunSigner(big.NewInt(int64(evmChainID))), sender.PriKey.EcdsaPrivateKey().(*ecdsa.PrivateKey))
		}
	}
	return tx, sender, err
}

var (
	testBlob       = kzg4844.Blob{1, 2, 3, 4}
	testBlobCommit = assertions.MustNoErrorV(kzg4844.BlobToCommitment(testBlob))
	testBlobProof  = assertions.MustNoErrorV(kzg4844.ComputeBlobProof(testBlob, testBlobCommit))
)

func generateSidecar(n int) *types.BlobTxSidecar {
	sidecar := &types.BlobTxSidecar{}
	for i := 0; i < n; i++ {
		sidecar.Blobs = append(sidecar.Blobs, testBlob)
		sidecar.Commitments = append(sidecar.Commitments, testBlobCommit)
		sidecar.Proofs = append(sidecar.Proofs, testBlobProof)
	}
	return sidecar
}
