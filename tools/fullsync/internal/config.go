// Package internal provides internal implementation for the fullsync detector tool
package internal

import (
	"path/filepath"

	"github.com/pkg/errors"

	"github.com/iotexproject/iotex-core/v2/config"
)

// Database type constants for different database types supported by the detector
const (
	// DatabaseTypeTrie represents the state trie database
	DatabaseTypeTrie = "trie"
	// DatabaseTypeContractStakingV1 represents the contract staking V1 index database
	DatabaseTypeContractStakingV1 = "contractstakingv1"
	// DatabaseTypeContractStakingV2 represents the contract staking V2 index database
	DatabaseTypeContractStakingV2 = "contractstakingv2"
	// DatabaseTypeContractStakingV3 represents the contract staking V3 index database
	DatabaseTypeContractStakingV3 = "contractstakingv3"
	// DatabaseTypeChain represents the blockchain database
	DatabaseTypeChain = "chain"
)

// DatabaseFile represents a database file to check
type DatabaseFile struct {
	Name   string
	Path   string
	Type   string
	Config *config.Config
}

// LoadConfig loads configuration from YAML file using IoTeX config structure
func LoadConfig(configPath string) (*config.Config, error) {
	cfg, err := config.New([]string{configPath}, nil) // Initialize with default values
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse config file")
	}
	return &cfg, nil
}

// GetDatabaseFiles extracts only TrieDB and ContractStakingIndexDB file paths from IoTeX config
func GetDatabaseFiles(cfg *config.Config) []DatabaseFile {
	var files []DatabaseFile

	// Helper function to add file if path is not empty
	addFile := func(name, path, dbType string) {
		if path != "" {
			// Convert to absolute path if it's relative
			if !filepath.IsAbs(path) {
				path, _ = filepath.Abs(path)
			}
			files = append(files, DatabaseFile{
				Name:   name,
				Path:   path,
				Type:   dbType,
				Config: cfg,
			})
		}
	}

	// Only support TrieDB and ContractStakingIndexDB
	addFile("Trie DB", cfg.Chain.TrieDBPath, DatabaseTypeTrie)
	addFile("Contract Staking V1 Index DB", cfg.Chain.ContractStakingIndexDBPath, DatabaseTypeContractStakingV1)
	addFile("Contract Staking V2 Index DB", cfg.Chain.ContractStakingIndexDBPath, DatabaseTypeContractStakingV2)
	addFile("Contract Staking V3 Index DB", cfg.Chain.ContractStakingIndexDBPath, DatabaseTypeContractStakingV3)
	addFile("Chain DB", cfg.Chain.ChainDBPath, DatabaseTypeChain)

	return files
}
