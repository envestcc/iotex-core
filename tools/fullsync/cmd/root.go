package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/iotexproject/iotex-core/v2/tools/fullsync/internal"
)

var (
	configPath   string
	outputFormat string
	detailedMode bool
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "fullsync-detector",
	Short: "IoTeX fullsync snapshot data file detector",
	Long: `A simplified tool to detect IoTeX fullsync snapshot data files and report their status.

This tool reads standard IoTeX node configuration files and checks the status of 
different database files. For each file found, it reports detailed information
specific to the database type. For missing files, it reports that the file was not found.

Supported databases:
- TrieDB: State trie database with height, account count, and compaction info
- ContractStakingV1/V2/V3: Contract staking index databases with staking statistics

You can use any existing IoTeX node configuration file with this tool.`,
	RunE: runDetector,
}

// Execute executes the root command
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Add command flags
	rootCmd.PersistentFlags().StringVarP(&configPath, "config", "c", "", "Path to configuration file (required)")
	rootCmd.PersistentFlags().StringVarP(&outputFormat, "output", "o", "text", "Output format: text, detailed, json, summary, csv")
	rootCmd.PersistentFlags().BoolVarP(&detailedMode, "detailed", "d", false, "Show detailed information for each database type")

	// Mark config flag as required
	rootCmd.MarkPersistentFlagRequired("config")
}

// runDetector is the main function that runs the detection
func runDetector(cmd *cobra.Command, args []string) error {
	// Load configuration
	config, err := internal.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %v", err)
	}

	// Get database files from config
	dbFiles := internal.GetDatabaseFiles(config)
	if len(dbFiles) == 0 {
		return fmt.Errorf("no database files found in configuration")
	}

	// Create detector and run detection
	detector := internal.NewDetector()
	result := detector.DetectFiles(dbFiles)

	// Override format if detailed mode is enabled
	format := outputFormat
	if detailedMode {
		format = "detailed"
	}

	// Output results using the new formatter
	formatter := internal.NewOutputFormatter(detector)
	return formatter.FormatOutput(result, format)
}
