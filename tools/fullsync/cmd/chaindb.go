// Package cmd contains the command line interface for the fullsync tools.
package cmd

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/iotexproject/iotex-core/v2/blockchain/block"
	"github.com/iotexproject/iotex-core/v2/blockchain/filedao"
	"github.com/iotexproject/iotex-core/v2/db"
)

var (
	evmNetworkID uint32
	heightStart  uint64
	heightEnd    uint64
)

func generateChainDB(basePath, outPath string, heightStart, heightEnd uint64) error {
	baseCfg := db.DefaultConfig
	baseCfg.DbPath = basePath
	baseCfg.ReadOnly = true
	baseDao, err := filedao.NewFileDAO(baseCfg, block.NewDeserializer(evmNetworkID))
	if err != nil {
		return err
	}
	ctx := context.Background()
	if err = baseDao.Start(ctx); err != nil {
		return err
	}
	defer baseDao.Stop(ctx)

	outCfg := db.DefaultConfig
	outCfg.DbPath = outPath
	outDao, err := filedao.NewFileDAOv2(heightStart, outCfg, block.NewDeserializer(evmNetworkID))
	if err != nil {
		return err
	}
	if err = outDao.Start(ctx); err != nil {
		return err
	}
	defer outDao.Stop(ctx)

	for height := heightStart; height <= heightEnd; height++ {
		blk, err := baseDao.GetBlockByHeight(height)
		if err != nil {
			return err
		}
		if err = outDao.PutBlock(ctx, blk); err != nil {
			return err
		}
	}
	return nil
}

var generateChainDBCmd = &cobra.Command{
	Use:   "generatechaindb [base-path] [output-path]",
	Short: "Generate chain database from existing database",
	Long: `Generate a new chain database by copying specific blocks from an existing database.
	
This command reads blocks from a base database and writes them to a new output database.
The blocks to copy are specified by the heights array.`,
	Args: cobra.ExactArgs(2),
	RunE: func(_ *cobra.Command, args []string) error {
		basePath := args[0]
		outPath := args[1]

		if heightStart == 0 && heightEnd == 0 {
			return fmt.Errorf("no heights specified, use --heights flag to specify block heights")
		}

		return generateChainDB(basePath, outPath, heightStart, heightEnd)
	},
}

func init() {
	generateChainDBCmd.Flags().Uint32Var(&evmNetworkID, "evm-network-id", 4689, "EVM network ID")
	generateChainDBCmd.Flags().Uint64Var(&heightStart, "height-start", 0, "Start block height (required)")
	generateChainDBCmd.Flags().Uint64Var(&heightEnd, "height-end", 0, "End block height (required)")
	generateChainDBCmd.MarkFlagRequired("height-start")
	generateChainDBCmd.MarkFlagRequired("height-end")
}
