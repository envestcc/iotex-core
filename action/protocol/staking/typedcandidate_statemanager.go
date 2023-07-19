// Copyright (c) 2023 IoTeX Foundation
// This source code is provided 'as is' and no warranties are given as to title or non-infringement, merchantability
// or fitness for purpose and, to the extent permitted by law, all liability for your use of the code is disclaimed.
// This source code is governed by Apache License 2.0 that can be found in the LICENSE file.

package staking

import (
	"github.com/iotexproject/iotex-core/action/protocol"
)

// typedCandidateStateManager is the state manager for typed candidate
type typedCandidateStateManager struct {
	protocol.StateManager
	*typedCandidateStateReader
}

// newTypedCandidateStateManager creates a new typed candidate state manager and loads all candidates from state
func newTypedCandidateStateManager(sm protocol.StateManager) (*typedCandidateStateManager, error) {
	csm := &typedCandidateStateManager{
		StateManager: sm,
	}
	var err error
	csm.typedCandidateStateReader, err = newTypedCandidateStateReader(sm)
	if err != nil {
		return nil, err
	}
	return csm, nil
}

func (csm *typedCandidateStateManager) add(cand *TypedCandidate) error {
	// update the candidate map
	candMap, ok := csm.candidateMap[cand.Type]
	if !ok {
		candMap = make(map[string]*TypedCandidate)
		csm.candidateMap[cand.Type] = candMap
	}
	candMap[cand.Operator.String()] = cand

	// update the state
	key := append(cand.Operator.Bytes(), byte(cand.Type))
	_, err := csm.PutState(cand, protocol.NamespaceOption(_typedCandidateNameSpace), protocol.KeyOption(key))
	return err
}
