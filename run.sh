#!/bin/bash

# Configuration
DB_PATH="/tmp/pebble-bench"
KEYS_FILE="./sample/keys.dat"
CONCURRENCY=4
READ_RATIO=1.0
RUNS=20
OUT_DIR="./logs"

mkdir -p "$OUT_DIR"

COMPLETED_LOGS=()

# Run benchmark multiple times
for i in $(seq 1 $RUNS); do
    ID="pebble-bench-$i"
    LOG_FILE="$OUT_DIR/$ID.logs"
    COMPLETED_LOGS+=("$LOG_FILE")

    echo "🔁 Run $i of $RUNS: $ID"

    go run main.go run \
        --db-path "$DB_PATH" \
        --keys-file "$KEYS_FILE" \
        --read-ratio "$READ_RATIO" \
        --concurrency "$CONCURRENCY" \
        --benchmark-id "$ID" \
        --log-format json \
        > "$LOG_FILE"

    echo "✅ Finished $ID, saved to $LOG_FILE"
done

# Prepare metrics accumulation
TOTAL_OPS=0
TOTAL_LAT=0
TOTAL_TIME=0
TOTAL_NOTFOUND=0
TOTAL_SUCCESSES=0
TOTAL_FAILURES=0
COUNT=0
TMP_ENTRIES=()

# Parse logs and accumulate
for file in "${COMPLETED_LOGS[@]}"; do
    entry=$(jq -c 'select(.message == "Read benchmark complete")' "$file")
    if [ -n "$entry" ]; then
        TMP_ENTRIES+=("$entry")
        OPS=$(echo "$entry" | jq '.read_ops_per_sec')
        LAT=$(echo "$entry" | jq '.read_avg_latency_ms')
        ELAPSED=$(echo "$entry" | jq '.read_total_elapsed')
        NOTFOUND=$(echo "$entry" | jq '.not_found')
        SUCCESSES=$(echo "$entry" | jq '.successful_reads')
        FAILURES=$(echo "$entry" | jq '.failed_reads')

        TOTAL_OPS=$(awk "BEGIN {print $TOTAL_OPS + $OPS}")
        TOTAL_LAT=$(awk "BEGIN {print $TOTAL_LAT + $LAT}")
        TOTAL_TIME=$(awk "BEGIN {print $TOTAL_TIME + $ELAPSED}")
        TOTAL_NOTFOUND=$(awk "BEGIN {print $TOTAL_NOTFOUND + $NOTFOUND}")
        TOTAL_SUCCESSES=$(awk "BEGIN {print $TOTAL_SUCCESSES + $SUCCESSES}")
        TOTAL_FAILURES=$(awk "BEGIN {print $TOTAL_FAILURES + $FAILURES}")
        
        COUNT=$((COUNT + 1))
    fi
done

# Compute averages
AVG_OPS=$(awk "BEGIN {print ($COUNT > 0) ? $TOTAL_OPS / $COUNT : 0}")
AVG_LAT=$(awk "BEGIN {print ($COUNT > 0) ? $TOTAL_LAT / $COUNT : 0}")
AVG_TIME=$(awk "BEGIN {print ($COUNT > 0) ? $TOTAL_TIME / $COUNT : 0}")
AVG_NOTFOUND=$(awk "BEGIN {print ($COUNT > 0) ? $TOTAL_NOTFOUND / $COUNT : 0}")
AVG_SUCCESSES=$(awk "BEGIN {print ($COUNT > 0) ? $TOTAL_SUCCESSES / $COUNT : 0}")
AVG_FAILURES=$(awk "BEGIN {print ($COUNT > 0) ? $TOTAL_FAILURES / $COUNT : 0}")

# Write final summary.json
SUMMARY_FILE="$OUT_DIR/summary.json"
{
  echo "{"
  echo "  \"avg_read_ops_per_sec\": $AVG_OPS,"
  echo "  \"avg_read_avg_latency_ms\": $AVG_LAT,"
  echo "  \"avg_read_total_elapsed\": $AVG_TIME,"
  echo "  \"avg_not_found\": $AVG_NOTFOUND,"
  echo "  \"avg_successful_reads\": $AVG_SUCCESSES,"
  echo "  \"avg_failed_reads\": $AVG_FAILURES,"
  echo "  \"runs\": ["
  for i in "${!TMP_ENTRIES[@]}"; do
    echo -n "    ${TMP_ENTRIES[$i]}"
    [[ $i -lt $((${#TMP_ENTRIES[@]} - 1)) ]] && echo "," || echo ""
  done
  echo "  ]"
  echo "}"
} > "$SUMMARY_FILE"

echo "📊 Final summary saved to $SUMMARY_FILE"
