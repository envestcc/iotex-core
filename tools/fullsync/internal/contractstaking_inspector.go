package internal

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/iotexproject/iotex-core/v2/action/protocol/staking"
	"github.com/iotexproject/iotex-core/v2/blockindex/contractstaking"
	"github.com/iotexproject/iotex-core/v2/config"
	"github.com/iotexproject/iotex-core/v2/db"
	"github.com/iotexproject/iotex-core/v2/systemcontractindex/stakingindex"
)

// ContractStakingInspector inspects ContractStaking databases
type ContractStakingInspector struct {
	version string // v1, v2, or v3
}

// ContractStakingDetails contains specific information for ContractStaking databases
type ContractStakingDetails struct {
	Height          uint64    `json:"height"`
	Version         string    `json:"version"`
	ContractAddress string    `json:"contractAddress"`
	DeployHeight    uint64    `json:"deployHeight"`
	LastUpdateTime  time.Time `json:"lastUpdateTime,omitempty"`
}

// NewContractStakingV1Inspector creates a new ContractStaking V1 inspector
func NewContractStakingV1Inspector() *ContractStakingInspector {
	return &ContractStakingInspector{version: "v1"}
}

// NewContractStakingV2Inspector creates a new ContractStaking V2 inspector
func NewContractStakingV2Inspector() *ContractStakingInspector {
	return &ContractStakingInspector{version: "v2"}
}

// NewContractStakingV3Inspector creates a new ContractStaking V3 inspector
func NewContractStakingV3Inspector() *ContractStakingInspector {
	return &ContractStakingInspector{version: "v3"}
}

// Inspect examines a ContractStaking database
func (c *ContractStakingInspector) Inspect(ctx context.Context, file DatabaseFile) (*DatabaseInfo, error) {
	info := &DatabaseInfo{
		Name:    file.Name,
		Path:    file.Path,
		Type:    file.Type,
		Details: make(map[string]interface{}),
	}

	// Check if file exists
	fileInfo, err := os.Stat(file.Path)
	if err != nil {
		if os.IsNotExist(err) {
			info.Error = "file not found"
			return info, nil
		}
		info.Error = fmt.Sprintf("failed to access file: %v", err)
		return info, nil
	}

	info.Exists = true
	info.Size = fileInfo.Size()
	info.ModTime = fileInfo.ModTime()

	// Get detailed ContractStaking information
	details, err := c.getContractStakingDetails(ctx, file)
	if err != nil {
		info.Error = fmt.Sprintf("failed to get contract staking details: %v", err)
		return info, nil
	}

	info.CustomData = details
	info.Details["height"] = details.Height
	info.Details["version"] = details.Version
	info.Details["contractAddress"] = details.ContractAddress
	info.Details["deployHeight"] = details.DeployHeight
	info.Details["lastUpdateTime"] = details.LastUpdateTime

	return info, nil
}

// GetDisplayFields returns the fields to display for ContractStaking
func (c *ContractStakingInspector) GetDisplayFields() []DisplayField {
	return []DisplayField{
		{
			Key:         "height",
			Label:       "Current Height",
			Type:        "number",
			Required:    true,
			Description: "Current blockchain height in the contract staking index",
		},
		{
			Key:         "version",
			Label:       "Contract Version",
			Type:        "string",
			Required:    true,
			Description: "Version of the staking contract",
		},
		{
			Key:         "contractAddress",
			Label:       "Contract Address",
			Type:        "string",
			Required:    true,
			Description: "Address of the staking contract",
		},
		{
			Key:         "deployHeight",
			Label:       "Deploy Height",
			Type:        "number",
			Required:    true,
			Description: "Height at which the contract was deployed",
		},
		{
			Key:         "totalBuckets",
			Label:       "Total Buckets",
			Type:        "number",
			Required:    false,
			Description: "Total number of staking buckets",
		},
		{
			Key:         "totalStaked",
			Label:       "Total Staked",
			Type:        "string",
			Required:    false,
			Description: "Total amount of IOTX staked",
		},
		{
			Key:         "activeDelegates",
			Label:       "Active Delegates",
			Type:        "number",
			Required:    false,
			Description: "Number of active delegates",
		},
		{
			Key:         "lastUpdateTime",
			Label:       "Last Update",
			Type:        "time",
			Required:    false,
			Description: "Time of last index update",
		},
		{
			Key:         "indexingStatus",
			Label:       "Indexing Status",
			Type:        "string",
			Required:    false,
			Description: "Current status of the indexing process",
		},
		{
			Key:         "voteWeightCalcType",
			Label:       "Vote Weight Calculation",
			Type:        "string",
			Required:    false,
			Description: "Type of vote weight calculation used",
		},
	}
}

// FormatDetails formats ContractStaking details for human-readable output
func (c *ContractStakingInspector) FormatDetails(info *DatabaseInfo) string {
	if !info.Exists {
		return "  ✗ File not found"
	}

	details, ok := info.CustomData.(*ContractStakingDetails)
	if !ok || details == nil {
		return "  ⚠ Could not retrieve detailed information"
	}

	output := fmt.Sprintf("  ✓ Height: %d\n", details.Height)
	output += fmt.Sprintf("  ✓ Contract Version: %s\n", details.Version)
	output += fmt.Sprintf("  ✓ Contract Address: %s\n", details.ContractAddress)
	output += fmt.Sprintf("  ✓ Deploy Height: %d\n", details.DeployHeight)

	if !details.LastUpdateTime.IsZero() {
		output += fmt.Sprintf("  ✓ Last Update: %s\n", details.LastUpdateTime.Format("2006-01-02 15:04:05"))
	}

	return output
}

// getContractStakingDetails retrieves detailed information from ContractStaking database
func (c *ContractStakingInspector) getContractStakingDetails(ctx context.Context, file DatabaseFile) (*ContractStakingDetails, error) {
	details := &ContractStakingDetails{Version: c.version}

	var height uint64
	var err error

	switch c.version {
	case "v1":
		height, err = c.getContractStakingHeightV1(file.Config)
		details.ContractAddress = file.Config.Genesis.SystemStakingContractAddress
		details.DeployHeight = file.Config.Genesis.SystemStakingContractHeight
	case "v2":
		height, err = c.getContractStakingHeightV2(file.Config)
		details.ContractAddress = file.Config.Genesis.SystemStakingContractV2Address
		details.DeployHeight = file.Config.Genesis.SystemStakingContractV2Height
	case "v3":
		height, err = c.getContractStakingHeightV3(file.Config)
		details.ContractAddress = file.Config.Genesis.SystemStakingContractV3Address
		details.DeployHeight = file.Config.Genesis.SystemStakingContractV3Height
	default:
		return nil, fmt.Errorf("unsupported contract staking version: %s", c.version)
	}

	if err != nil {
		return nil, err
	}

	details.Height = height
	details.LastUpdateTime = time.Now() // This could be read from the database

	// Try to get additional information (best effort)
	c.tryGetAdditionalInfo(file, details)

	return details, nil
}

// getContractStakingHeightV1 gets height from contract staking V1 index database
func (c *ContractStakingInspector) getContractStakingHeightV1(cfg *config.Config) (uint64, error) {
	dbConfig := cfg.DB
	dbConfig.DbPath = cfg.Chain.ContractStakingIndexDBPath
	kvstore := db.NewBoltDB(dbConfig)
	blockDurationFn := func(start uint64, end uint64, viewAt uint64) time.Duration {
		if viewAt < cfg.Genesis.WakeBlockHeight {
			return time.Duration(end-start) * cfg.DardanellesUpgrade.BlockInterval
		}
		return time.Duration(end-start) * cfg.WakeUpgrade.BlockInterval
	}
	voteCalcConsts := cfg.Genesis.VoteWeightCalConsts
	indexer, err := contractstaking.NewContractStakingIndexer(
		kvstore,
		contractstaking.Config{
			ContractAddress:      cfg.Genesis.SystemStakingContractAddress,
			ContractDeployHeight: cfg.Genesis.SystemStakingContractHeight,
			CalculateVoteWeight: func(v *staking.VoteBucket) *big.Int {
				return staking.CalculateVoteWeight(voteCalcConsts, v, false)
			},
			BlocksToDuration: blockDurationFn,
		})
	if err != nil {
		return 0, err
	}
	if err = indexer.Start(context.Background()); err != nil {
		return 0, err
	}
	defer indexer.Stop(context.Background())

	return indexer.Height()
}

// getContractStakingHeightV2 gets height from contract staking V2 index database
func (c *ContractStakingInspector) getContractStakingHeightV2(cfg *config.Config) (uint64, error) {
	dbConfig := cfg.DB
	dbConfig.DbPath = cfg.Chain.ContractStakingIndexDBPath
	kvstore := db.NewBoltDB(dbConfig)
	blockDurationFn := func(start uint64, end uint64, viewAt uint64) time.Duration {
		if viewAt < cfg.Genesis.WakeBlockHeight {
			return time.Duration(end-start) * cfg.DardanellesUpgrade.BlockInterval
		}
		return time.Duration(end-start) * cfg.WakeUpgrade.BlockInterval
	}
	indexer := stakingindex.NewIndexer(
		kvstore,
		cfg.Genesis.SystemStakingContractV2Address,
		cfg.Genesis.SystemStakingContractV2Height,
		blockDurationFn,
		stakingindex.WithMuteHeight(cfg.Genesis.WakeBlockHeight),
	)
	if err := indexer.Start(context.Background()); err != nil {
		return 0, err
	}
	defer indexer.Stop(context.Background())

	return indexer.Height()
}

// getContractStakingHeightV3 gets height from contract staking V3 index database
func (c *ContractStakingInspector) getContractStakingHeightV3(cfg *config.Config) (uint64, error) {
	dbConfig := cfg.DB
	dbConfig.DbPath = cfg.Chain.ContractStakingIndexDBPath
	kvstore := db.NewBoltDB(dbConfig)
	blockDurationFn := func(start uint64, end uint64, viewAt uint64) time.Duration {
		if viewAt < cfg.Genesis.WakeBlockHeight {
			return time.Duration(end-start) * cfg.DardanellesUpgrade.BlockInterval
		}
		return time.Duration(end-start) * cfg.WakeUpgrade.BlockInterval
	}
	indexer := stakingindex.NewIndexer(
		kvstore,
		cfg.Genesis.SystemStakingContractV3Address,
		cfg.Genesis.SystemStakingContractV3Height,
		blockDurationFn,
		stakingindex.WithMuteHeight(cfg.Genesis.WakeBlockHeight),
	)
	if err := indexer.Start(context.Background()); err != nil {
		return 0, err
	}
	defer indexer.Stop(context.Background())

	return indexer.Height()
}

// tryGetAdditionalInfo attempts to get additional information (best effort)
func (c *ContractStakingInspector) tryGetAdditionalInfo(file DatabaseFile, details *ContractStakingDetails) {
	if file.Path != "" {
		if fileInfo, err := os.Stat(file.Path); err == nil {
			// Rough estimate: assume each bucket takes about 512 bytes
			details.LastUpdateTime = fileInfo.ModTime()
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
