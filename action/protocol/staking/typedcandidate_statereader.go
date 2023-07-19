// Copyright (c) 2023 IoTeX Foundation
// This source code is provided 'as is' and no warranties are given as to title or non-infringement, merchantability
// or fitness for purpose and, to the extent permitted by law, all liability for your use of the code is disclaimed.
// This source code is governed by Apache License 2.0 that can be found in the LICENSE file.

package staking

import (
	"github.com/iotexproject/iotex-address/address"
	"github.com/pkg/errors"

	"github.com/iotexproject/iotex-core/action/protocol"
	"github.com/iotexproject/iotex-core/state"
)

// typedCandidateStateReader is the state reader for typed candidate
type typedCandidateStateReader struct {
	protocol.StateReader
	candidateMap map[CandidateType]map[string]*TypedCandidate
}

// newTypedCandidateStateReader creates a new typed candidate state reader and loads all candidates from state
func newTypedCandidateStateReader(sr protocol.StateReader) (*typedCandidateStateReader, error) {
	csr := &typedCandidateStateReader{
		StateReader:  sr,
		candidateMap: make(map[CandidateType]map[string]*TypedCandidate),
	}
	if err := csr.loadFromState(); err != nil {
		return nil, err
	}
	return csr, nil
}

func (csr *typedCandidateStateReader) loadFromState() error {
	candidateMap := csr.candidateMap
	_, iter, err := csr.States(protocol.NamespaceOption(_typedCandidateNameSpace))
	if err != nil {
		if errors.Is(err, state.ErrStateNotExist) {
			return nil
		}
		return err
	}
	cands := []*TypedCandidate{}
	for i := 0; i < iter.Size(); i++ {
		c := &TypedCandidate{}
		if err := iter.Next(c); err != nil {
			return errors.Wrapf(err, "failed to deserialize execution candidate")
		}
		cands = append(cands, c)
	}
	for i := range cands {
		candMap, ok := candidateMap[cands[i].Type]
		if !ok {
			candMap = make(map[string]*TypedCandidate)
			candidateMap[cands[i].Type] = candMap
		}
		candMap[cands[i].Operator.String()] = cands[i]
	}
	return nil
}

func (csr *typedCandidateStateReader) has(candType CandidateType, operatorAddr address.Address) bool {
	candMap, ok := csr.candidateMap[candType]
	if !ok {
		return false
	}
	_, ok = candMap[operatorAddr.String()]
	return ok
}
