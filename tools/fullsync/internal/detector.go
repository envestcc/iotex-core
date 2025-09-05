package internal

import (
	"context"
	"time"
)

// DetectResult represents the detection result
type DetectResult struct {
	Timestamp time.Time      `json:"timestamp"`
	Status    string         `json:"status"`
	Files     []DatabaseInfo `json:"files"`
	Summary   Summary        `json:"summary"`
}

// Summary provides a summary of the detection
type Summary struct {
	TotalFiles     int `json:"totalFiles"`
	FilesFound     int `json:"filesFound"`
	FilesMissing   int `json:"filesMissing"`
	FilesWithError int `json:"filesWithError"`
}

// Detector handles database file detection using the new inspector architecture
type Detector struct {
	registry *InspectorRegistry
}

// NewDetector creates a new detector with registered inspectors
func NewDetector() *Detector {
	return &Detector{
		registry: NewInspectorRegistry(),
	}
}

// DetectFiles detects all database files and their status using appropriate inspectors
func (d *Detector) DetectFiles(files []DatabaseFile) *DetectResult {
	result := &DetectResult{
		Timestamp: time.Now(),
		Status:    "success",
		Files:     make([]DatabaseInfo, len(files)),
		Summary:   Summary{TotalFiles: len(files)},
	}

	ctx := context.Background()

	for i, file := range files {
		inspector, err := d.registry.GetInspector(file.Type)
		if err != nil {
			// Fallback to basic file info if no inspector is available
			info := &DatabaseInfo{
				Name:   file.Name,
				Path:   file.Path,
				Type:   file.Type,
				Error:  err.Error(),
				Exists: false,
			}
			result.Files[i] = *info
			result.Summary.FilesWithError++
			result.Status = "partial_failure"
			continue
		}

		info, err := inspector.Inspect(ctx, file)
		if err != nil {
			info = &DatabaseInfo{
				Name:   file.Name,
				Path:   file.Path,
				Type:   file.Type,
				Error:  err.Error(),
				Exists: false,
			}
		}

		result.Files[i] = *info

		if info.Exists {
			result.Summary.FilesFound++
		} else {
			result.Summary.FilesMissing++
		}

		if info.Error != "" {
			result.Summary.FilesWithError++
			result.Status = "partial_failure"
		}
	}

	return result
}

// GetSupportedDatabaseTypes returns all supported database types
func (d *Detector) GetSupportedDatabaseTypes() []string {
	return d.registry.GetSupportedTypes()
}

// GetDisplayFields returns display fields for a specific database type
func (d *Detector) GetDisplayFields(dbType string) ([]DisplayField, error) {
	inspector, err := d.registry.GetInspector(dbType)
	if err != nil {
		return nil, err
	}
	return inspector.GetDisplayFields(), nil
}
