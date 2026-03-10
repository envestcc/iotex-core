#!/bin/bash

# Analyze PebbleDB namespace sizes - v2
# Usage: ./analyze_pebble_v2.sh <pebble_db_path>

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

# First, let's see what pebble db space actually outputs
echo ""
echo "Debug: Testing pebble db space output format..."
echo "---"
$PEBBLE_BIN db space "$DB_PATH" --start "cbc6b0367695d68d" --end "cbc6b0367695d68dffffffffffffffff" 2>&1
echo "---"

echo ""
echo "Debug: Checking total DB properties..."
$PEBBLE_BIN db properties "$DB_PATH" 2>&1

echo ""
echo "=================================================="
echo "Now counting keys per namespace (this may take a while)..."
echo "=================================================="

declare -A NAMESPACES=(
    ["Account"]="cbc6b0367695d68d cbc6b0367695d68dffffffffffffffff"
    ["Code"]="e5e207ce28e9642a e5e207ce28e9642affffffffffffffff"
    ["Contract"]="6dbfee27776f259e 6dbfee27776f259effffffffffffffff"
    ["System"]="ff44ff9e93a2d5ec ff44ff9e93a2d5ecffffffffffffffff"
    ["Staking"]="71a666ea8a3e434c 71a666ea8a3e434cffffffffffffffff"
    ["Candidate"]="2ecd9aed9981f838 2ecd9aed9981f838ffffffffffffffff"
    ["Rewarding"]="a2668df9e920f239 a2668df9e920f239ffffffffffffffff"
)

printf "%-20s %15s\n" "Namespace" "Key Count"
echo "--------------------------------------------------"

for ns in "${!NAMESPACES[@]}"; do
    read -r start end <<< "${NAMESPACES[$ns]}"
    count=$($PEBBLE_BIN db scan "$DB_PATH" --start "$start" --end "$end" --count 0 2>/dev/null | wc -l | tr -d ' ')
    printf "%-20s %15s\n" "$ns" "$count"
done

echo "--------------------------------------------------"
echo ""
echo "Total DB Size on disk:"
du -sh "$DB_PATH"
