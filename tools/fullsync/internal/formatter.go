package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// OutputFormatter handles different output formats
type OutputFormatter struct {
	detector *Detector
}

// NewOutputFormatter creates a new output formatter
func NewOutputFormatter(detector *Detector) *OutputFormatter {
	return &OutputFormatter{detector: detector}
}

// FormatOutput formats the detection result according to the specified format
func (f *OutputFormatter) FormatOutput(result *DetectResult, format string) error {
	switch strings.ToLower(format) {
	case "json":
		return f.outputJSON(result)
	case "text", "":
		return f.outputText(result)
	case "detailed":
		return f.outputDetailed(result)
	case "summary":
		return f.outputSummary(result)
	case "csv":
		return f.outputCSV(result)
	default:
		return fmt.Errorf("unsupported output format: %s", format)
	}
}

// outputJSON outputs results in JSON format
func (f *OutputFormatter) outputJSON(result *DetectResult) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

// outputText outputs results in human-readable text format
func (f *OutputFormatter) outputText(result *DetectResult) error {
	fmt.Println("Database File Status Check Report")
	fmt.Println("==================================")
	fmt.Printf("Timestamp: %s\n", result.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Printf("Status: %s\n\n", result.Status)

	for _, file := range result.Files {
		fmt.Printf("%s (%s):\n", file.Name, file.Path)

		if file.Exists {
			fmt.Printf("  ✓ File exists (size: %s)\n", formatSize(file.Size))
			if !file.ModTime.IsZero() {
				fmt.Printf("  ✓ Last modified: %s\n", file.ModTime.Format("2006-01-02 15:04:05"))
			}

			// Display basic information
			if height, exists := file.Details["height"]; exists {
				if h, ok := height.(uint64); ok {
					fmt.Printf("  ✓ Current height: %d\n", h)
				}
			}
		} else {
			fmt.Printf("  ✗ File not found\n")
		}

		if file.Error != "" {
			fmt.Printf("  ⚠ Warning: %s\n", file.Error)
		}

		fmt.Println()
	}

	f.printSummary(result)
	return nil
}

// outputDetailed outputs results with detailed information specific to each database type
func (f *OutputFormatter) outputDetailed(result *DetectResult) error {
	fmt.Println("Detailed Database File Status Report")
	fmt.Println("=====================================")
	fmt.Printf("Timestamp: %s\n", result.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Printf("Status: %s\n\n", result.Status)

	for _, file := range result.Files {
		fmt.Printf("%s (%s):\n", file.Name, file.Path)
		fmt.Printf("Type: %s\n", file.Type)

		if file.Exists {
			fmt.Printf("  ✓ File exists (size: %s)\n", formatSize(file.Size))
			if !file.ModTime.IsZero() {
				fmt.Printf("  ✓ Last modified: %s\n", file.ModTime.Format("2006-01-02 15:04:05"))
			}

			// Try to get inspector for detailed formatting
			if displayFields, err := f.detector.GetDisplayFields(file.Type); err == nil {
				fmt.Printf("\nDetailed Information:\n")
				f.displayFieldsWithValues(displayFields, file.Details)
			} else {
				// Fallback to basic details
				f.displayBasicDetails(file.Details)
			}
		} else {
			fmt.Printf("  ✗ File not found\n")
		}

		if file.Error != "" {
			fmt.Printf("  ⚠ Error: %s\n", file.Error)
		}

		fmt.Printf("\n" + strings.Repeat("-", 50) + "\n\n")
	}

	f.printSummary(result)
	return nil
}

// outputSummary outputs a brief summary
func (f *OutputFormatter) outputSummary(result *DetectResult) error {
	fmt.Printf("Database Status Summary (%s)\n", result.Timestamp.Format("2006-01-02 15:04:05"))
	fmt.Println(strings.Repeat("=", 40))

	f.printSummary(result)

	fmt.Println("\nDatabase Types:")
	typeCount := make(map[string]int)
	for _, file := range result.Files {
		typeCount[file.Type]++
	}

	for dbType, count := range typeCount {
		fmt.Printf("  %s: %d files\n", dbType, count)
	}

	return nil
}

// outputCSV outputs results in CSV format
func (f *OutputFormatter) outputCSV(result *DetectResult) error {
	// CSV Header
	fmt.Println("Name,Path,Type,Exists,Size,Height,LastModified,Error")

	for _, file := range result.Files {
		height := ""
		if h, exists := file.Details["height"]; exists {
			if heightVal, ok := h.(uint64); ok {
				height = fmt.Sprintf("%d", heightVal)
			}
		}

		modTime := ""
		if !file.ModTime.IsZero() {
			modTime = file.ModTime.Format("2006-01-02 15:04:05")
		}

		fmt.Printf("\"%s\",\"%s\",\"%s\",%t,%d,\"%s\",\"%s\",\"%s\"\n",
			file.Name, file.Path, file.Type, file.Exists, file.Size, height, modTime, file.Error)
	}

	return nil
}

// displayFieldsWithValues displays fields with their values based on field definitions
func (f *OutputFormatter) displayFieldsWithValues(fields []DisplayField, details map[string]interface{}) {
	for _, field := range fields {
		if value, exists := details[field.Key]; exists && value != nil {
			switch field.Type {
			case "number":
				if num, ok := value.(uint64); ok && (num > 0 || field.Required) {
					fmt.Printf("  ✓ %s: %d\n", field.Label, num)
				} else if num, ok := value.(int); ok && (num > 0 || field.Required) {
					fmt.Printf("  ✓ %s: %d\n", field.Label, num)
				}
			case "string":
				if str, ok := value.(string); ok && (str != "" || field.Required) {
					fmt.Printf("  ✓ %s: %s\n", field.Label, str)
				}
			case "time":
				if t, ok := value.(time.Time); ok && (!t.IsZero() || field.Required) {
					fmt.Printf("  ✓ %s: %s\n", field.Label, t.Format("2006-01-02 15:04:05"))
				}
			case "size":
				if size, ok := value.(int64); ok && (size > 0 || field.Required) {
					fmt.Printf("  ✓ %s: %s\n", field.Label, formatSize(size))
				}
			case "custom":
				if field.Key == "blockRanges" {
					if ranges, ok := value.([]string); ok && len(ranges) > 0 {
						fmt.Printf("  ✓ %s:\n", field.Label)
						for _, rangeStr := range ranges {
							fmt.Printf("    - %s\n", rangeStr)
						}
					} else if field.Required {
						fmt.Printf("  ✗ %s: not available\n", field.Label)
					}
				} else if field.Required {
					fmt.Printf("  ✓ %s: %v\n", field.Label, value)
				}
			default:
				if field.Required {
					fmt.Printf("  ✓ %s: %v\n", field.Label, value)
				}
			}
		} else if field.Required {
			fmt.Printf("  ✗ %s: not available\n", field.Label)
		}
	}
}

// displayBasicDetails displays basic details when no inspector is available
func (f *OutputFormatter) displayBasicDetails(details map[string]interface{}) {
	for key, value := range details {
		if value != nil {
			fmt.Printf("  ✓ %s: %v\n", strings.Title(key), value)
		}
	}
}

// printSummary prints the summary section
func (f *OutputFormatter) printSummary(result *DetectResult) {
	fmt.Println("Summary:")
	fmt.Printf("- Total files checked: %d\n", result.Summary.TotalFiles)
	fmt.Printf("- Files found: %d\n", result.Summary.FilesFound)
	fmt.Printf("- Files missing: %d\n", result.Summary.FilesMissing)
	if result.Summary.FilesWithError > 0 {
		fmt.Printf("- Files with errors: %d\n", result.Summary.FilesWithError)
	}

	if result.Summary.FilesMissing == 0 && result.Summary.FilesWithError == 0 {
		fmt.Printf("- All found files are healthy: ✓\n")
	}
}

// formatSize formats file size in human-readable format
func formatSize(size int64) string {
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
