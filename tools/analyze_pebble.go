package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/iotexproject/go-pkgs/hash"
)

const prefixLength = 8

func nsToPrefix(ns string) []byte {
	h := hash.Hash160b([]byte(ns))
	return h[:prefixLength]
}

func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: analyze_pebble <db_path>")
		fmt.Println("Example: analyze_pebble /var/data/chain.db")
		os.Exit(1)
	}

	dbPath := os.Args[1]

	opts := &pebble.Options{
		ReadOnly: true,
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	fmt.Printf("Analyzing PebbleDB at: %s\n", dbPath)
	fmt.Println("==================================================")
	fmt.Println()

	namespaces := []string{
		"Account",
		"Code",
		"Contract",
		"System",
		"Staking",
		"Candidate",
		"CandsMap",
		"StakingView",
		"Rewarding",
		"AccountKV",
		"PollCandidates",
		"PollProbation",
		"PollUnproductive",
		"S2SBuckets",
		"S2SBucketTypes",
		"ContractStakingBucket",
		"ContractStakingBucketType",
		"StakingContractMeta",
	}

	type nsStats struct {
		name      string
		keyCount  int64
		keySize   int64
		valueSize int64
	}

	results := make([]nsStats, 0, len(namespaces))
	var totalCount, totalKeySize, totalValueSize int64

	for _, ns := range namespaces {
		prefix := nsToPrefix(ns)
		start := prefix
		end := make([]byte, prefixLength+8)
		copy(end, prefix)
		for i := prefixLength; i < len(end); i++ {
			end[i] = 0xff
		}

		var keyCount, keySize, valueSize int64

		iterOpts := &pebble.IterOptions{
			LowerBound: start,
			UpperBound: end,
		}
		iter, _ := db.NewIter(iterOpts)

		startTime := time.Now()
		lastReport := startTime

		for iter.First(); iter.Valid(); iter.Next() {
			keyCount++
			keySize += int64(len(iter.Key()))
			valueSize += int64(len(iter.Value()))

			if keyCount%1000000 == 0 || time.Since(lastReport) > 10*time.Second {
				elapsed := time.Since(startTime).Seconds()
				rate := float64(keyCount) / elapsed
				fmt.Printf("\r  [%s] Scanned %d keys (%.0f keys/sec)...     ", ns, keyCount, rate)
				lastReport = time.Now()
			}
		}

		if keyCount > 0 {
			fmt.Printf("\r  [%s] Completed: %d keys, %s data          \n",
				ns, keyCount, formatSize(keySize+valueSize))
		} else {
			fmt.Printf("\r  [%s] No keys found                              \n", ns)
		}

		if err := iter.Error(); err != nil {
			fmt.Printf("  [%s] Iterator error: %v\n", ns, err)
		}
		iter.Close()

		results = append(results, nsStats{
			name:      ns,
			keyCount:  keyCount,
			keySize:   keySize,
			valueSize: valueSize,
		})

		totalCount += keyCount
		totalKeySize += keySize
		totalValueSize += valueSize
	}

	fmt.Println()
	fmt.Println("==================================================")
	fmt.Println("Results by Namespace (sorted by size):")
	fmt.Println("--------------------------------------------------------------------------")
	fmt.Printf("%-30s %15s %12s %12s %12s\n", "Namespace", "Key Count", "Key Size", "Value Size", "Total")
	fmt.Println("--------------------------------------------------------------------------")

	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].keySize+results[j].valueSize > results[i].keySize+results[i].valueSize {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	for _, r := range results {
		total := r.keySize + r.valueSize
		fmt.Printf("%-30s %15d %12s %12s %12s\n",
			r.name,
			r.keyCount,
			formatSize(r.keySize),
			formatSize(r.valueSize),
			formatSize(total),
		)
	}

	fmt.Println("--------------------------------------------------------------------------")
	fmt.Printf("%-30s %15d %12s %12s %12s\n",
		"TOTAL (known namespaces)",
		totalCount,
		formatSize(totalKeySize),
		formatSize(totalValueSize),
		formatSize(totalKeySize+totalValueSize),
	)
	fmt.Println()

	var dirSize int64
	filepath.Walk(dbPath, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			dirSize += info.Size()
		}
		return nil
	})
	fmt.Printf("Total DB directory size: %s\n", formatSize(dirSize))
	fmt.Printf("Unaccounted space: %s\n", formatSize(dirSize-totalKeySize-totalValueSize))
}
