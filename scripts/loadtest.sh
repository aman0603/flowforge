#!/usr/bin/env bash
#
# loadtest.sh - simple API load test for FlowForge.
#
# Uses `hey` (https://github.com/rakyll/hey) if available, otherwise falls back
# to a bounded curl loop. This is a developer convenience for local throughput
# checks; it is NOT run in CI and requires a running FlowForge stack.
#
# Usage:
#   BASE_URL=http://localhost:8080 REQUESTS=2000 CONCURRENCY=50 ./scripts/loadtest.sh
#
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
REQUESTS="${REQUESTS:-1000}"
CONCURRENCY="${CONCURRENCY:-25}"
TARGET_PATH="${TARGET_PATH:-/healthz}"
URL="${BASE_URL}${TARGET_PATH}"

echo "FlowForge load test"
echo "  url:         ${URL}"
echo "  requests:    ${REQUESTS}"
echo "  concurrency: ${CONCURRENCY}"
echo

if command -v hey >/dev/null 2>&1; then
  exec hey -n "${REQUESTS}" -c "${CONCURRENCY}" "${URL}"
fi

echo "hey not found; using curl fallback (no percentile stats)."
start=$(date +%s)
success=0
fail=0
for ((i = 0; i < REQUESTS; i++)); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "${URL}" || echo "000")
  if [[ "${code}" == "200" ]]; then
    success=$((success + 1))
  else
    fail=$((fail + 1))
  fi
done
end=$(date +%s)
elapsed=$((end - start))
[[ "${elapsed}" -eq 0 ]] && elapsed=1

echo "  completed:   ${success} ok / ${fail} failed"
echo "  elapsed:     ${elapsed}s"
echo "  throughput:  $((REQUESTS / elapsed)) req/s (approx)"
