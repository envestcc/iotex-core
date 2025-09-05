package internal

import (
	"context"
	"fmt"
	"time"
)

// DatabaseInfo represents detailed information about a database
type DatabaseInfo struct {
	Name       string                 `json:"name"`
	Path       string                 `json:"path"`
	Type       string                 `json:"type"`
	Exists     bool                   `json:"exists"`
	Size       int64                  `json:"size,omitempty"`
	ModTime    time.Time              `json:"modTime,omitempty"`
	Error      string                 `json:"error,omitempty"`
	Details    map[string]interface{} `json:"details,omitempty"`
	CustomData interface{}            `json:"customData,omitempty"`
}

// DatabaseInspector interface for different database types
type DatabaseInspector interface {
	// Inspect examines the database and returns detailed information
	Inspect(ctx context.Context, file DatabaseFile) (*DatabaseInfo, error)

	// GetDisplayFields returns the fields that should be displayed for this database type
	GetDisplayFields() []DisplayField

	// FormatDetails formats the details for human-readable output
	FormatDetails(info *DatabaseInfo) string
}

// DisplayField defines how a field should be displayed
type DisplayField struct {
	Key         string `json:"key"`
	Label       string `json:"label"`
	Type        string `json:"type"` // "number", "string", "time", "size", "custom"
	Required    bool   `json:"required"`
	Description string `json:"description"`
}

// InspectorRegistry manages database inspectors
type InspectorRegistry struct {
	inspectors map[string]DatabaseInspector
}

// NewInspectorRegistry creates a new inspector registry
func NewInspectorRegistry() *InspectorRegistry {
	registry := &InspectorRegistry{
		inspectors: make(map[string]DatabaseInspector),
	}

	// Register default inspectors
	registry.Register(DatabaseTypeTrie, NewTrieInspector())
	registry.Register(DatabaseTypeContractStakingV1, NewContractStakingV1Inspector())
	registry.Register(DatabaseTypeContractStakingV2, NewContractStakingV2Inspector())
	registry.Register(DatabaseTypeContractStakingV3, NewContractStakingV3Inspector())
	registry.Register(DatabaseTypeChain, NewChainInspector())

	return registry
}

// Register registers an inspector for a database type
func (r *InspectorRegistry) Register(dbType string, inspector DatabaseInspector) {
	r.inspectors[dbType] = inspector
}

// GetInspector returns the inspector for a database type
func (r *InspectorRegistry) GetInspector(dbType string) (DatabaseInspector, error) {
	inspector, exists := r.inspectors[dbType]
	if !exists {
		return nil, fmt.Errorf("no inspector registered for database type: %s", dbType)
	}
	return inspector, nil
}

// GetSupportedTypes returns all supported database types
func (r *InspectorRegistry) GetSupportedTypes() []string {
	types := make([]string, 0, len(r.inspectors))
	for dbType := range r.inspectors {
		types = append(types, dbType)
	}
	return types
}
