#!/usr/bin/env bash
# Kill -9 / zero-loss / zero-duplicate chaos run (IMPLEMENTATION-PLAN §M1
# acceptance bullet 2). Starts observatoryd against a tempdir DB, publishes
# a seeded firehose, SIGKILLs the daemon mid-flight at a randomized
# instant, restarts it, lets the generator resume from acks, then verifies
# count + checksum against the store.
#
# Usage: scripts/chaos-killnine.sh [EVENTS]   (default 100000 — the CI
# smoke size; the full local run uses 1000000)
set -euo pipefail

EVENTS="${1:-100000}"
SEED="${CHAOS_SEED:-1337}"
RUN_ID="chaos-$SEED"
PORT_GRPC=17470
PORT_HTTP=17471

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

start_daemon() {
  ./target/release/observatoryd --config "$WORK/config.toml" >> "$WORK/daemon.log" 2>&1 &
  DAEMON_PID=$!
  timeout 30 bash -c "until curl -sf -o /dev/null http://127.0.0.1:$PORT_HTTP/healthz; do sleep 0.2; done"
}

echo "chaos: starting daemon"
start_daemon

echo "chaos: publishing $EVENTS events (seed $SEED) with resume"
./target/release/obs-events-gen publish \
  --addr "http://127.0.0.1:$PORT_GRPC" \
  --seed "$SEED" --events "$EVENTS" --vocab catalog \
  --run-id "$RUN_ID" --producer-id "chaos-producer" \
  --rate 10000 --resume &
PUBLISH_PID=$!

# Randomized kill instant somewhere in the middle of the flight.
SLEEP_MS=$(( (RANDOM % 2000) + 500 ))
sleep "$(awk "BEGIN {print $SLEEP_MS/1000}")"
echo "chaos: kill -9 daemon (pid $DAEMON_PID) after ${SLEEP_MS}ms"
kill -9 "$DAEMON_PID"
sleep 0.5

echo "chaos: restarting daemon"
start_daemon

echo "chaos: waiting for publisher to finish resuming"
wait "$PUBLISH_PID"

# Graceful stop so the WAL is checkpointed before direct DB reads.
kill -TERM "$DAEMON_PID"
wait "$DAEMON_PID" 2>/dev/null || true

echo "chaos: verifying zero loss / zero duplicates"
./target/release/obs-events-gen verify \
  --db "$WORK/observatory.db" \
  --seed "$SEED" --events "$EVENTS" --vocab catalog \
  --run-id "$RUN_ID" --producer-id "chaos-producer"

echo "chaos: OK ($EVENTS events, kill -9 survived, zero loss, zero duplicates)"
