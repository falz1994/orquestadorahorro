#!/usr/bin/env bash
set -euo pipefail

# Recreates only the named combo. Credentials remain in OmniRoute's private
# environment and are never written by this script.
if ! command -v omniroute >/dev/null 2>&1; then
  echo "ERROR: omniroute is not installed or not on PATH" >&2
  exit 2
fi

SPEC='[{"model":"ocwrap/big-pickle","providerId":"ocwrap"},{"model":"ocwrap/longcat-2.5-preview-free","providerId":"ocwrap"},{"model":"cohere/north-mini-code:free","providerId":"openrouter"},{"model":"nex-agi/nex-n2.5-pro:free","providerId":"openrouter"},{"model":"gemini/gemini-flash-lite-latest","providerId":"gemini"}]'

omniroute combo delete code --yes >/dev/null 2>&1 || true
omniroute combo create code --strategy priority --models "$SPEC"

echo
echo "Configured combo code: OpenCode -> OpenRouter -> Gemini"
