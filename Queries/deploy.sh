#!/usr/bin/env bash
# Deploy the SIGMOD 2025 demo queries against a local single-node worker.
#
#   ./Queries/deploy.sh            deploy all eight queries
#   ./Queries/deploy.sh Q3 Q7      deploy only the named ones
#
# Runs everything inside the dev container, so the only prerequisite is a built
# build-docker/ tree and a running Docker daemon. Results land in Output/.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${NES_IMAGE:-nebulastream/nes-development:local}"
QUERIES=("$@")
if [ ${#QUERIES[@]} -eq 0 ]; then
    QUERIES=(Q1 Q2 Q3 Q4 Q5 Q6 Q7 Q8)
fi

mkdir -p "$REPO/Output"

docker run --rm -v "$REPO":/work -w /work "$IMAGE" bash -c '
set -uo pipefail
WORKER=./build-docker/nes-single-node-worker/nes-single-node-worker
CLI=./build-docker/nes-frontend/apps/nes-cli

for bin in "$WORKER" "$CLI"; do
    [ -x "$bin" ] || { echo "missing $bin - run cmake --build build-docker first" >&2; exit 1; }
done

# Config keys take --key=value form; bare "--key value" is rejected.
"$WORKER" --grpc=localhost:8080 --data_address=localhost:9090 > /tmp/worker.log 2>&1 &
WPID=$!
trap "kill $WPID 2>/dev/null" EXIT

for i in $(seq 1 30); do
    grep -q "Server listening" /tmp/worker.log 2>/dev/null && break
    sleep 1
done
grep -q "Server listening" /tmp/worker.log || { echo "worker failed to start:" >&2; tail -20 /tmp/worker.log >&2; exit 1; }

for q in '"${QUERIES[*]}"'; do
    id=$("$CLI" -t "Queries/$q.yaml" start 2>/dev/null | tail -1)
    printf "%-4s submitted as %s\n" "$q" "$id"
    sleep 6
done

# Let the last windows close and flush before tearing the worker down.
sleep 8

echo
echo "Results in Output/ (first line of each file is the schema header):"
for f in Output/*.json; do
    [ -e "$f" ] || continue
    printf "  %-40s %s rows\n" "$(basename "$f")" "$(( $(wc -l < "$f") - 1 ))"
done
'
