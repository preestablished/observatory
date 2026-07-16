#!/usr/bin/env bash
# 30-second throughput smoke at 10k events/s (the CI-sized variant of the
# M1 sustained-throughput acceptance; the 10-minute p99 number is
# Spark-hardware local evidence). Asserts only: no publish error, all
# events acked, writer channel depth stays bounded.
set -euo pipefail

RATE=10000
SECONDS_TO_RUN=30
EVENTS=$((RATE * SECONDS_TO_RUN))
SEED=777
RUN_ID="throughput-$SEED"
PORT_GRPC=17472
PORT_HTTP=17473

cd "$(dirname "$0")/.."
cargo build --release -p observatoryd -p obs-events-gen

WORK="$(mktemp -d)"
trap 'kill -9 "${DAEMON_PID:-0}" 2>/dev/null || true; rm -rf "$WORK"' EXIT

cat > "$WORK/config.toml" <<EOF
version = 1
[server]
grpc_listen = "127.0.0.1:$PORT_GRPC"
http_listen = "127.0.0.1:$PORT_HTTP"
[storage]
path = "$WORK/observatory.db"
EOF

./target/release/observatoryd --config "$WORK/config.toml" >> "$WORK/daemon.log" 2>&1 &
DAEMON_PID=$!
timeout 30 bash -c "until curl -sf -o /dev/null http://127.0.0.1:$PORT_HTTP/healthz; do sleep 0.2; done"

# Sample channel depth every second while publishing.
MAX_DEPTH=0
(
  for _ in $(seq "$((SECONDS_TO_RUN + 10))"); do
    DEPTH=$(curl -sf "http://127.0.0.1:$PORT_HTTP/metrics" | awk '/^obs_ingest_channel_depth /{print $2}') || DEPTH=0
    echo "${DEPTH:-0}"
    sleep 1
  done
) > "$WORK/depths.txt" &
SAMPLER_PID=$!

./target/release/obs-events-gen publish \
  --addr "http://127.0.0.1:$PORT_GRPC" \
  --seed "$SEED" --events "$EVENTS" --vocab catalog \
  --run-id "$RUN_ID" --producer-id "throughput-producer" \
  --rate "$RATE"

kill "$SAMPLER_PID" 2>/dev/null || true
wait "$SAMPLER_PID" 2>/dev/null || true
MAX_DEPTH=$(sort -n "$WORK/depths.txt" | tail -1)
echo "throughput: max observed channel depth = ${MAX_DEPTH:-0} (bound 4096)"
if [ "${MAX_DEPTH:-0}" -ge 4096 ]; then
  echo "throughput: FAIL — channel depth hit the bound (backpressure saturated)"
  exit 1
fi

kill -TERM "$DAEMON_PID"
wait "$DAEMON_PID" 2>/dev/null || true

./target/release/obs-events-gen verify \
  --db "$WORK/observatory.db" \
  --seed "$SEED" --events "$EVENTS" --vocab catalog \
  --run-id "$RUN_ID" --producer-id "throughput-producer"

echo "throughput: OK ($EVENTS events at ${RATE}/s, depth bounded)"
