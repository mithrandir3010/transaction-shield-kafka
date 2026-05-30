#!/usr/bin/env bash
# ── K6 Load Test Runner ─────────────────────────────────────────────────────
# Usage:
#   ./load-tests/k6/run.sh baseline                   # default SLO test
#   ./load-tests/k6/run.sh stress                     # ramp to 500 VU
#   ./load-tests/k6/run.sh spike                      # burst test
#   ./load-tests/k6/run.sh soak                       # 30-min endurance
#   BASE_URL=http://staging:8082 ./run.sh baseline    # against non-local env
#   SOAK_DURATION=60m ./run.sh soak                   # longer soak
#
# Prerequisites:
#   brew install k6   (macOS)
#   apt-get install k6  (Debian/Ubuntu)
#   Docker + docker-compose up  (all local infrastructure)
#
# Output formats:
#   Default: human-readable summary
#   JSON:    k6 run --out json=results.json ...
#   InfluxDB: k6 run --out influxdb=http://localhost:8086/k6 ...
# ────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCENARIO="${1:-baseline}"

VALID_SCENARIOS=("baseline" "stress" "spike" "soak")
if [[ ! " ${VALID_SCENARIOS[*]} " =~ " ${SCENARIO} " ]]; then
    echo "Unknown scenario: ${SCENARIO}"
    echo "Valid: ${VALID_SCENARIOS[*]}"
    exit 1
fi

if ! command -v k6 &>/dev/null; then
    echo "k6 not found. Install: brew install k6"
    exit 1
fi

echo "═══════════════════════════════════════════════════"
echo "  Transaction Shield — K6 Load Test"
echo "  Scenario   : ${SCENARIO}"
echo "  Target     : ${BASE_URL:-http://localhost:8082}"
echo "  JWT Secret : ${JWT_SECRET_KEY:+[from env]}"
echo "  JWT Secret : ${JWT_SECRET_KEY:-[using dev default]}"
echo "═══════════════════════════════════════════════════"

k6 run \
    ${BASE_URL:+-e BASE_URL="${BASE_URL}"} \
    ${JWT_SECRET_KEY:+-e JWT_SECRET_KEY="${JWT_SECRET_KEY}"} \
    ${SOAK_DURATION:+-e SOAK_DURATION="${SOAK_DURATION}"} \
    "${SCRIPT_DIR}/scenarios/${SCENARIO}.js"
