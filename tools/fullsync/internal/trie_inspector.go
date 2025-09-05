package internal

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/iotexproject/iotex-core/v2/config"
	"github.com/iotexproject/iotex-core/v2/db"
	"github.com/iotexproject/iotex-core/v2/state/factory"
)

// TrieInspector inspects TrieDB databases
type TrieInspector struct{}

// TrieDetails contains specific information for TrieDB
type TrieDetails struct {
	Height uint64 `json:"height"`
}

// CompactionStats contains database compaction statistics
type CompactionStats struct {
	LastCompaction time.Time `json:"lastCompaction,omitempty"`
	CompactionSize int64     `json:"compactionSize,omitempty"`
}

// NewTrieInspector creates a new TrieInspector
func NewTrieInspector() *TrieInspector {
	return &TrieInspector{}
}

// Inspect examines a TrieDB database
func (t *TrieInspector) Inspect(ctx context.Context, file DatabaseFile) (*DatabaseInfo, error) {
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

	// Get detailed TrieDB information
	details, err := t.getTrieDetails(ctx, file)
	if err != nil {
		info.Error = fmt.Sprintf("failed to get trie details: %v", err)
		return info, nil
	}

	info.CustomData = details
	info.Details["height"] = details.Height

	return info, nil
}

// GetDisplayFields returns the fields to display for TrieDB
func (t *TrieInspector) GetDisplayFields() []DisplayField {
	return []DisplayField{
		{
			Key:         "height",
			Label:       "Current Height",
			Type:        "number",
			Required:    true,
			Description: "Current blockchain height in the trie database",
		},
		{
			Key:         "stateRoots",
			Label:       "State Roots",
			Type:        "number",
			Required:    false,
			Description: "Number of state roots stored",
		},
		{
			Key:         "accountCount",
			Label:       "Account Count",
			Type:        "number",
			Required:    false,
			Description: "Approximate number of accounts in the state",
		},
		{
			Key:         "lastCheckpoint",
			Label:       "Last Checkpoint",
			Type:        "number",
			Required:    false,
			Description: "Height of the last checkpoint",
		},
		{
			Key:         "dbVersion",
			Label:       "DB Version",
			Type:        "string",
			Required:    false,
			Description: "Database schema version",
		},
		{
			Key:         "lastCompaction",
			Label:       "Last Compaction",
			Type:        "time",
			Required:    false,
			Description: "Time of last database compaction",
		},
		{
			Key:         "compactionSize",
			Label:       "Compaction Size",
			Type:        "size",
			Required:    false,
			Description: "Size reduced during last compaction",
		},
	}
}

// FormatDetails formats TrieDB details for human-readable output
func (t *TrieInspector) FormatDetails(info *DatabaseInfo) string {
	if !info.Exists {
		return "  ✗ File not found"
	}

	details, ok := info.CustomData.(*TrieDetails)
	if !ok || details == nil {
		return "  ⚠ Could not retrieve detailed information"
	}

	output := fmt.Sprintf("  ✓ Height: %d\n", details.Height)

	return output
}

// getTrieDetails retrieves detailed information from TrieDB
func (t *TrieInspector) getTrieDetails(ctx context.Context, file DatabaseFile) (*TrieDetails, error) {
	details := &TrieDetails{}

	// Get height using the existing method
	height, err := t.getTrieHeight(file.Config)
	if err != nil {
		return nil, err
	}
	details.Height = height

	// Try to get additional information (these are optional and best-effort)
	t.tryGetAdditionalInfo(file, details)

	return details, nil
}

// getTrieHeight gets height from trie database
func (t *TrieInspector) getTrieHeight(cfg *config.Config) (uint64, error) {
	factoryCfg := factory.GenerateConfig(cfg.Chain, cfg.Genesis)
	factoryDBCfg := cfg.DB
	factoryDBCfg.DBType = cfg.Chain.FactoryDBType

	opts := []factory.StateDBOption{
		factory.DefaultPatchOption(),
	}

	dao, err := db.CreateKVStore(factoryDBCfg, cfg.Chain.TrieDBPath)
	if err != nil {
		return 0, err
	}

	statedb, err := factory.NewStateDB(factoryCfg, dao, opts...)
	if err != nil {
		return 0, err
	}

	err = statedb.Start(context.Background())
	if err != nil {
		return 0, err
	}
	defer statedb.Stop(context.Background())

	return statedb.Height()
}

// tryGetAdditionalInfo attempts to get additional information (best effort)
func (t *TrieInspector) tryGetAdditionalInfo(file DatabaseFile, details *TrieDetails) {
	return
}
