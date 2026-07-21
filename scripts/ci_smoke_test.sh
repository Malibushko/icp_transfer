#!/usr/bin/env bash
# Runs the producer and consumer against a shared ring for a couple of seconds
# and asserts the consumer saw packets with no CRC errors and no sequence gaps.
set -euo pipefail

BIN_DIR="${1:-build}"
SHM="/pkt_ci_$$"
PROD_LOG="$(mktemp)"
CONS_LOG="$(mktemp)"

PP=""
CP=""
cleanup() {
    [[ -n "$PP" ]] && kill "$PP" 2>/dev/null || true
    [[ -n "$CP" ]] && kill "$CP" 2>/dev/null || true
    rm -f "/dev/shm/${SHM#/}" "$PROD_LOG" "$CONS_LOG" 2>/dev/null || true
}
trap cleanup EXIT

"$BIN_DIR/producer" 1024 "$SHM" 8 >"$PROD_LOG" 2>&1 &
PP=$!
sleep 0.5
"$BIN_DIR/consumer" "$SHM" >"$CONS_LOG" 2>&1 &
CP=$!

sleep 2
kill -INT "$PP" "$CP" 2>/dev/null || true
wait "$PP" 2>/dev/null || true
wait "$CP" 2>/dev/null || true

echo "----- producer -----"; cat "$PROD_LOG"
echo "----- consumer -----"; cat "$CONS_LOG"
echo "--------------------"

summary="$(grep 'done: total=' "$CONS_LOG" || true)"
[[ -n "$summary" ]]            || { echo "FAIL: no consumer summary line"; exit 1; }
grep -q 'crc_err=0' <<<"$summary" || { echo "FAIL: CRC errors detected";  exit 1; }
grep -q 'seq_gap=0' <<<"$summary" || { echo "FAIL: sequence gaps detected"; exit 1; }

total="$(sed -E 's/.*total=([0-9]+).*/\1/' <<<"$summary")"
[[ "${total:-0}" -gt 0 ]]      || { echo "FAIL: no packets transferred"; exit 1; }

echo "PASS: $total packets, crc_err=0, seq_gap=0"
