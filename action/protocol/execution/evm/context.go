package evm

import (
	"context"

	"github.com/iotexproject/iotex-core/pkg/log"
)

type (
	helperContextKey struct{}

	// helperContext is the context for EVM helper
	helperContext struct {
		getBlockTime GetBlockTime
	}
)

// WithHelperCtx returns a new context with helper context
func WithHelperCtx(ctx context.Context, getBlockTime GetBlockTime) context.Context {
	return context.WithValue(ctx, helperContextKey{}, helperContext{
		getBlockTime: getBlockTime,
	})
}

// mustGetHelperCtx returns the helper context from the context
func mustGetHelperCtx(ctx context.Context) helperContext {
	hc, ok := ctx.Value(helperContextKey{}).(helperContext)
	if !ok {
		log.S().Panic("Miss evm helper context")
	}
	return hc
}
