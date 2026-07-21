#!/bin/sh
BUILD=${1:-build}
SECS=${2:-6}
cd "$(dirname "$0")/.." || exit 1

command -v taskset >/dev/null && PIN_P="taskset -c 2" PIN_C="taskset -c 4" || PIN_P="" PIN_C=""

printf "%10s %10s | %14s %12s\n" "payload_B" "ring_MiB" "peak_pkt/s" "peak_MB/s"
for ring in 8 64 256; do
    for payload in 512 4096 16384 65536 262144; do
        $PIN_C "$BUILD/consumer" /bench_ring </dev/null 2>/tmp/bench_consumer.log &
        cpid=$!
        sleep 0.3
        $PIN_P timeout "$SECS" "$BUILD/producer" "$payload" /bench_ring "$ring" </dev/null 2>/dev/null
        sleep 0.3
        kill -INT $cpid 2>/dev/null
        wait $cpid 2>/dev/null
        # skip first (warm-up) and last (partial) windows
        best=$(grep 'pkt/s' /tmp/bench_consumer.log | head -n -1 | tail -n +2 \
            | awk -F'|' '{gsub(/[^0-9.]/,"",$2); gsub(/\(.*/,"",$3); gsub(/[^0-9.]/,"",$3);
                          if ($3+0 > mb) { mb=$3; pk=$2 } } END { printf "%14s %12s", pk, mb }')
        printf "%10s %10s | %s\n" "$payload" "$ring" "$best"
    done
done
