package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iotexproject/iotex-core/v2/blockchain/filedao"
	"github.com/iotexproject/iotex-core/v2/db"
)

// ChainInspector inspects Chain databases
type ChainInspector struct{}

// ChainDetails contains specific information for Chain databases
type ChainDetails struct {
	Height        uint64          `json:"height"`
	TotalFiles    int             `json:"totalFiles"`
	MainFile      *ChainFileInfo  `json:"mainFile,omitempty"`
	ShardFiles    []ChainFileInfo `json:"shardFiles,omitempty"`
	TotalSize     int64           `json:"totalSize"`
	FormatVersion string          `json:"formatVersion,omitempty"`
	HeightRanges  []HeightRange   `json:"heightRanges,omitempty"`
}

// ChainFileInfo contains information about a single chain database file
type ChainFileInfo struct {
	Path        string    `json:"path"`
	Size        int64     `json:"size"`
	ModTime     time.Time `json:"modTime"`
	HeightStart uint64    `json:"heightStart,omitempty"`
	HeightEnd   uint64    `json:"heightEnd,omitempty"`
	BlockCount  uint64    `json:"blockCount,omitempty"`
	Version     string    `json:"version,omitempty"`
	IsMainFile  bool      `json:"isMainFile"`
	ShardIndex  uint64    `json:"shardIndex,omitempty"`
}

// HeightRange represents a range of block heights
type HeightRange struct {
	Start      uint64 `json:"start"`
	End        uint64 `json:"end"`
	FileIndex  uint64 `json:"fileIndex,omitempty"`
	FileName   string `json:"fileName"`
	BlockCount uint64 `json:"blockCount"`
}

// NewChainInspector creates a new ChainInspector
func NewChainInspector() *ChainInspector {
	return &ChainInspector{}
}

// Inspect examines a Chain database and its shards
func (c *ChainInspector) Inspect(ctx context.Context, file DatabaseFile) (*DatabaseInfo, error) {
	info := &DatabaseInfo{
		Name:    file.Name,
		Path:    file.Path,
		Type:    file.Type,
		Details: make(map[string]interface{}),
	}

	// Check if main file exists
	fileInfo, err := os.Stat(file.Path)
	if err != nil {
		if os.IsNotExist(err) {
			info.Error = "main file not found"
			return info, nil
		}
		info.Error = fmt.Sprintf("failed to access main file: %v", err)
		return info, nil
	}

	info.Exists = true
	info.Size = fileInfo.Size()
	info.ModTime = fileInfo.ModTime()

	// Get detailed Chain information including shards
	details, err := c.getChainDetails(ctx, file)
	if err != nil {
		info.Error = fmt.Sprintf("failed to get chain details: %v", err)
		return info, nil
	}

	// Update the total size to include all files
	info.Size = details.TotalSize

	info.CustomData = details
	info.Details["height"] = details.Height

	// Format block ranges for display
	var blockRanges []string
	for _, hr := range details.HeightRanges {
		if hr.FileIndex == 0 {
			blockRanges = append(blockRanges, fmt.Sprintf("Main: %d-%d (%d blocks)",
				hr.Start, hr.End, hr.BlockCount))
		} else {
			blockRanges = append(blockRanges, fmt.Sprintf("Shard %d: %d-%d (%d blocks)",
				hr.FileIndex, hr.Start, hr.End, hr.BlockCount))
		}
	}
	info.Details["blockRanges"] = blockRanges

	return info, nil
}

// GetDisplayFields returns the fields to display for Chain databases
func (c *ChainInspector) GetDisplayFields() []DisplayField {
	return []DisplayField{
		{
			Key:         "height",
			Label:       "Current Height",
			Type:        "number",
			Required:    true,
			Description: "Current blockchain height in the chain database",
		},
		{
			Key:         "blockRanges",
			Label:       "Block Ranges",
			Type:        "custom",
			Required:    true,
			Description: "Block height ranges contained in the database files",
		},
	}
}

// FormatDetails formats Chain details for human-readable output
func (c *ChainInspector) FormatDetails(info *DatabaseInfo) string {
	if !info.Exists {
		return "  ✗ Main file not found"
	}

	details, ok := info.CustomData.(*ChainDetails)
	if !ok || details == nil {
		return "  ⚠ Could not retrieve detailed information"
	}

	output := fmt.Sprintf("  ✓ Height: %d\n", details.Height)

	// Show block ranges
	if len(details.HeightRanges) > 0 {
		output += "  ✓ Block Ranges:\n"
		for _, hr := range details.HeightRanges {
			if hr.FileIndex == 0 {
				output += fmt.Sprintf("    - Main: %d-%d (%d blocks)\n",
					hr.Start, hr.End, hr.BlockCount)
			} else {
				output += fmt.Sprintf("    - Shard %d: %d-%d (%d blocks)\n",
					hr.FileIndex, hr.Start, hr.End, hr.BlockCount)
			}
		}
	}

	return output
}

// getChainDetails retrieves detailed information from Chain database including shards
func (c *ChainInspector) getChainDetails(_ context.Context, file DatabaseFile) (*ChainDetails, error) {
	details := &ChainDetails{
		TotalFiles:   1, // At least the main file
		ShardFiles:   make([]ChainFileInfo, 0),
		HeightRanges: make([]HeightRange, 0),
	}

	// Get main file info
	mainFileInfo, err := c.inspectSingleFile(file.Path, 0, true)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect main file: %v", err)
	}
	details.MainFile = mainFileInfo
	details.TotalSize = mainFileInfo.Size
	details.Height = mainFileInfo.HeightEnd
	details.FormatVersion = mainFileInfo.Version

	// Add main file to height ranges
	if mainFileInfo.BlockCount > 0 {
		details.HeightRanges = append(details.HeightRanges, HeightRange{
			Start:      mainFileInfo.HeightStart,
			End:        mainFileInfo.HeightEnd,
			FileIndex:  0,
			FileName:   filepath.Base(file.Path),
			BlockCount: mainFileInfo.BlockCount,
		})
	}

	// Look for shard files
	shardFiles, err := c.findShardFiles(file.Path)
	if err != nil {
		fmt.Println("failed to find shard files:", err)
		// This is not critical, continue without shard info
		return details, nil
	}

	// Inspect each shard file
	for _, shardPath := range shardFiles {
		shardIndex, _ := c.extractShardIndex(shardPath, file.Path)
		shardInfo, err := c.inspectSingleFile(shardPath, shardIndex, false)
		if err != nil {
			// Log error but continue with other files
			continue
		}

		details.ShardFiles = append(details.ShardFiles, *shardInfo)
		details.TotalSize += shardInfo.Size
		details.TotalFiles++

		// Update overall height if this shard has higher blocks
		if shardInfo.HeightEnd > details.Height {
			details.Height = shardInfo.HeightEnd
		}

		// Add to height ranges
		if shardInfo.BlockCount > 0 {
			details.HeightRanges = append(details.HeightRanges, HeightRange{
				Start:      shardInfo.HeightStart,
				End:        shardInfo.HeightEnd,
				FileIndex:  shardIndex,
				FileName:   filepath.Base(shardPath),
				BlockCount: shardInfo.BlockCount,
			})
		}
	}

	// Sort height ranges by start height
	sort.Slice(details.HeightRanges, func(i, j int) bool {
		return details.HeightRanges[i].Start < details.HeightRanges[j].Start
	})

	return details, nil
}

// inspectSingleFile inspects a single chain database file
func (c *ChainInspector) inspectSingleFile(filePath string, shardIndex uint64, isMain bool) (*ChainFileInfo, error) {
	info := &ChainFileInfo{
		Path:       filePath,
		ShardIndex: shardIndex,
		IsMainFile: isMain,
	}

	// Get file stats
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	info.Size = fileInfo.Size()
	info.ModTime = fileInfo.ModTime()

	// Try to open as filedao to get version and height info
	fileHeader, err := c.readFileHeader(filePath)
	if err != nil {
		return info, err
	}
	info.Version = fileHeader.Version
	info.HeightStart = fileHeader.Start
	info.HeightEnd = fileHeader.Start + fileHeader.BlockStoreSize
	info.BlockCount = fileHeader.BlockStoreSize

	return info, nil
}

// findShardFiles finds all shard files for a given main chain database file
func (c *ChainInspector) findShardFiles(mainFilePath string) ([]string, error) {
	dir := filepath.Dir(mainFilePath)
	base := filepath.Base(mainFilePath)

	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var shardFiles []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		if c.isShardFile(file.Name(), base) {
			shardPath := filepath.Join(dir, file.Name())
			shardFiles = append(shardFiles, shardPath)
		}
	}

	// Sort shard files by index
	sort.Strings(shardFiles)
	return shardFiles, nil
}

// isShardFile checks if a filename is a shard file of the given base file
func (c *ChainInspector) isShardFile(filename, baseFile string) bool {
	// Check if filename matches pattern: base-NNNNNNNN.ext
	ext := filepath.Ext(baseFile)
	baseWithoutExt := strings.TrimSuffix(baseFile, ext)
	filenameWithoutExt := strings.TrimSuffix(filename, filepath.Ext(filename))

	if !strings.HasPrefix(filenameWithoutExt, baseWithoutExt+"-") {
		return false
	}

	suffix := strings.TrimPrefix(filenameWithoutExt, baseWithoutExt+"-")
	if len(suffix) != 8 { // -NNNNNNNN format
		return false
	}

	// Check if suffix is a valid number
	if _, err := strconv.Atoi(suffix); err != nil {
		return false
	}

	return filepath.Ext(filename) == ext
}

// extractShardIndex extracts the shard index from a shard file path
func (c *ChainInspector) extractShardIndex(shardPath, mainPath string) (uint64, error) {
	shardFile := filepath.Base(shardPath)
	mainFile := filepath.Base(mainPath)

	ext := filepath.Ext(mainFile)
	baseWithoutExt := strings.TrimSuffix(mainFile, ext)
	shardWithoutExt := strings.TrimSuffix(shardFile, filepath.Ext(shardFile))

	suffix := strings.TrimPrefix(shardWithoutExt, baseWithoutExt+"-")
	index, err := strconv.Atoi(suffix)
	if err != nil {
		return 0, err
	}

	return uint64(index), nil
}

// readFileHeader reads the file header using filedao utilities
func (c *ChainInspector) readFileHeader(filename string) (*filedao.FileHeader, error) {
	fileType := filedao.FileAll
	file := db.NewBoltDB(db.Config{DbPath: filename, NumRetries: 3})
	ctx := context.Background()
	if err := file.Start(ctx); err != nil {
		// not a valid db file
		return nil, filedao.ErrFileInvalid
	}
	defer file.Stop(ctx)

	switch fileType {
	case filedao.FileLegacyMaster, filedao.FileLegacyAuxiliary:
		return filedao.ReadHeaderLegacy(file)
	case filedao.FileV2:
		if headerV2, err := filedao.ReadHeaderV2(file); err == nil {
			return headerV2, nil
		}
		return nil, filedao.ErrFileInvalid
	case filedao.FileAll:
		if header, err := filedao.ReadHeaderLegacy(file); err == nil {
			return header, nil
		}
		if headerV2, err := filedao.ReadHeaderV2(file); err == nil {
			return headerV2, nil
		}
		return nil, filedao.ErrFileInvalid
	default:
		panic(fmt.Errorf("unsupported check type %s", fileType))
	}
}

// getFileHeight gets the current height from a chain database file
func (c *ChainInspector) getFileHeight(dbConfig db.Config) (uint64, error) {
	// Try to open the database and read the height
	kvStore := db.NewBoltDB(dbConfig)

	ctx := context.Background()
	if err := kvStore.Start(ctx); err != nil {
		return 0, err
	}
	defer kvStore.Stop(ctx)

	// Try to read the top height key (this might need adjustment based on actual filedao implementation)
	topHeightKey := []byte("th")
	value, err := kvStore.Get("hdr", topHeightKey) // header namespace
	if err != nil {
		return 0, err
	}

	if len(value) != 8 {
		return 0, fmt.Errorf("invalid height value length")
	}

	// Convert bytes to uint64 (little endian)
	height := uint64(value[0]) | uint64(value[1])<<8 | uint64(value[2])<<16 | uint64(value[3])<<24 |
		uint64(value[4])<<32 | uint64(value[5])<<40 | uint64(value[6])<<48 | uint64(value[7])<<56

	return height, nil
}

// formatFileSize formats file size in human-readable format
func formatFileSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}
