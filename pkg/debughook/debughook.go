// Package debughook exposes env-var-driven hooks for narrowing verbose diagnostics
// to a single target block/action, used to trace the mainnet ioID device-registration
// revert (iotexinternal/iotex-servers issue #194) during a trie.db catch-up.
//
// This package is intentionally free of iotex-core internal imports so it can be
// referenced from anywhere (state factory, EVM adapter, blockdao) without cycles.
//
// Configuration is read once at process startup from environment variables:
//
//	IOID_DEBUG_TARGET_BLOCK   — decimal block height where verbose logging fires.
//	                             Empty or "0" disables the debug hook entirely.
//	IOID_DEBUG_TARGET_TX      — hex action hash (without "0x") whose ExecuteContract
//	                             call should also flip the per-action verbose flag.
//	IOID_DEBUG_PANIC_BEFORE_COMMIT — "1" to panic in the state-factory PutBlock right
//	                             before ws.Commit for the target block, so trie.db
//	                             stays at height-1 for post-mortem inspection.
package debughook

import (
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

var (
	targetBlock         uint64
	targetActionHash    [32]byte
	targetActionHashSet bool
	panicBeforeCommit   bool

	// activeAction is set to 1 while ExecuteContract is running the target tx,
	// so the EVM statedb adapter can gate its per-op logging on it.
	activeAction atomic.Int32
)

func init() {
	if h := strings.TrimSpace(os.Getenv("IOID_DEBUG_TARGET_BLOCK")); h != "" && h != "0" {
		if n, err := strconv.ParseUint(h, 10, 64); err == nil {
			targetBlock = n
		}
	}
	if t := strings.TrimSpace(os.Getenv("IOID_DEBUG_TARGET_TX")); t != "" {
		t = strings.TrimPrefix(t, "0x")
		if b, err := hex.DecodeString(t); err == nil && len(b) == 32 {
			copy(targetActionHash[:], b)
			targetActionHashSet = true
		}
	}
	if v := strings.TrimSpace(os.Getenv("IOID_DEBUG_PANIC_BEFORE_COMMIT")); v == "1" || v == "true" {
		panicBeforeCommit = true
	}
}

// Enabled reports whether any debug hook is armed.
func Enabled() bool { return targetBlock != 0 || targetActionHashSet }

// TargetBlock returns the configured block height (0 if unset).
func TargetBlock() uint64 { return targetBlock }

// PanicBeforeCommit reports whether to panic before ws.Commit at the target block.
func PanicBeforeCommit() bool { return panicBeforeCommit }

// IsTargetBlock reports whether the given height matches the configured target.
func IsTargetBlock(h uint64) bool { return targetBlock != 0 && h == targetBlock }

// IsTargetAction reports whether the given action hash matches the configured
// target (32-byte hex without "0x"). Accepts anything convertible to [32]byte,
// including hash.Hash256 aliases.
func IsTargetAction[T ~[32]byte](h T) bool {
	if !targetActionHashSet {
		return false
	}
	return [32]byte(h) == targetActionHash
}

// EnterAction marks the target-action window as active. Nested / concurrent
// re-entries are permitted; matching ExitAction calls are required to fully
// clear the flag.
func EnterAction() { activeAction.Add(1) }

// ExitAction leaves the target-action window.
func ExitAction() { activeAction.Add(-1) }

// InTargetAction reports whether execution is currently inside the target
// action's ExecuteContract call. Cheap enough to gate hot-path log calls on.
func InTargetAction() bool { return activeAction.Load() > 0 }
