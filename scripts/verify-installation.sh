#!/usr/bin/env bash
set -euo pipefail

command -v omniroute >/dev/null || { echo "ERROR: omniroute not found" >&2; exit 2; }
systemctl --user is-active --quiet omniroute.service || {
  echo "ERROR: omniroute.service is not active" >&2
  exit 1
}

omniroute combo list --json

echo
echo "Preset files:"
for file in agent.cordis.yml preset.yml orchestrator-readonly-policy-v2.js orchestrator-job-watchdog.js omniroute-limit-observer.js; do
  test -f "$(dirname "$0")/../orchestrator/$file" || { echo "MISSING: $file" >&2; exit 1; }
  echo "  OK $file"
done

echo
echo "No credential-bearing files are part of this bundle."
