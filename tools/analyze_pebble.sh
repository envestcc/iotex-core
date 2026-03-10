#!/bin/bash

# Analyze PebbleDB namespace sizes
# Usage: ./analyze_pebble.sh <pebble_db_path>
#
# Prerequisites:
#   1. Install pebble CLI: go install github.com/cockroachdb/pebble/cmd/pebble@latest
#   2. Stop iotex service before running (database requires exclusive access)
#
# Example:
#   ./analyze_pebble.sh /var/data/chain.db

PEBBLE_BIN="$HOME/go/bin/pebble"
DB_PATH="${1:-/var/data/chain.db}"

if [ ! -f "$PEBBLE_BIN" ]; then
    echo "Error: Pebble CLI not found at $PEBBLE_BIN"
    echo "Please install it first: go install github.com/cockroachdb/pebble/cmd/pebble@latest"
    exit 1
fi

if [ ! -d "$DB_PATH" ]; then
    echo "Error: Database path not found: $DB_PATH"
    exit 1
fi

echo "Analyzing PebbleDB at: $DB_PATH"
echo "=================================================="
echo ""

# Define namespaces with their hex prefixes (8-byte prefix + ffffffff... for end)
declare -A NAMESPACES=(
    ["Account"]="cbc6b0367695d68d cbc6b0367695d68dffffffffffffffff"
    ["Code"]="e5e207ce28e9642a e5e207ce28e9642affffffffffffffff"
    ["Contract"]="6dbfee27776f259e 6dbfee27776f259effffffffffffffff"
    ["System"]="ff44ff9e93a2d5ec ff44ff9e93a2d5ecffffffffffffffff"
    ["Staking"]="71a666ea8a3e434c 71a666ea8a3e434cffffffffffffffff"
    ["Candidate"]="2ecd9aed9981f838 2ecd9aed9981f838ffffffffffffffff"
    ["CandsMap"]="1b595627ff48e4d7 1b595627ff48e4d7ffffffffffffffff"
    ["StakingView"]="4e6fd35d430cd193 4e6fd35d430cd193ffffffffffffffff"
    ["Rewarding"]="a2668df9e920f239 a2668df9e920f239ffffffffffffffff"
    ["AccountKV"]="933df3fe3dc619c4 933df3fe3dc619c4ffffffffffffffff"
    ["PollCandidates"]="ff492b7f4b3cde9e ff492b7f4b3cde9effffffffffffffff"
    ["PollProbation"]="0704c20edd51c1d7 0704c20edd51c1d7ffffffffffffffff"
    ["PollUnproductive"]="e24f23a355344de7 e24f23a355344de7ffffffffffffffff"
    ["S2SBuckets"]="7326cd81443aa71d 7326cd81443aa71dffffffffffffffff"
    ["S2SBucketTypes"]="a82df4659a8938d8 a82df4659a8938d8ffffffffffffffff"
    ["ContractStakingBucket"]="1b6a963922bac898 1b6a963922bac898ffffffffffffffff"
    ["ContractStakingBucketType"]="10fd7a2c5c245cf3 10fd7a2c5c245cf3ffffffffffffffff"
    ["StakingContractMeta"]="a5cddd0eb8682af0 a5cddd0eb8682af0ffffffffffffffff"
)

echo "Namespace Space Usage:"
echo "--------------------------------------------------"
printf "%-30s %15s\n" "Namespace" "Size"
echo "--------------------------------------------------"

# Store results for sorting
declare -a results=()

for ns in "${!NAMESPACES[@]}"; do
    read -r start end <<< "${NAMESPACES[$ns]}"
    size=$($PEBBLE_BIN db space "$DB_PATH" --start "$start" --end "$end" 2>/dev/null | tr -d '\n')
    if [ -n "$size" ]; then
        results+=("$size|$ns")
    else
        results+=("0 B|$ns")
    fi
done

# Sort by size (largest first) and print
IFS=$'\n' sorted=($(for item in "${results[@]}"; do echo "$item"; done | sort -t'|' -k1 -hr))
unset IFS

total_size=0
for item in "${sorted[@]}"; do
    size="${item%%|*}"
    ns="${item##*|}"
    printf "%-30s %15s\n" "$ns" "$size"
done

echo "--------------------------------------------------"
echo ""
echo "Total DB Size on disk:"
du -sh "$DB_PATH"
echo ""
echo "LSM Properties:"
$PEBBLE_BIN db properties "$DB_PATH" 2>/dev/null | head -20
echo ""
echo "Note: Database must not be in use by another process."
