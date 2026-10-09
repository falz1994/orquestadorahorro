#!/usr/bin/env bash
set -euo pipefail

# OmniRoute publishes the runtime in a compiled Next bundle. This patch keeps
# the provider-scoped Gemini policy effective in both source and active bundle.
PKG="${OMNIROUTE_PKG:-$HOME/.npm-global/lib/node_modules/omniroute}"
REASONING_SOURCE="$PKG/open-sse/services/defaultReasoningEffort.ts"
TRANSLATOR_SOURCE="$PKG/open-sse/translator/request/openai-to-gemini.ts"
BUNDLE="$PKG/dist/.build/next/server/chunks"
STAMP="$(date +%Y%m%d-%H%M%S)"

for path in "$REASONING_SOURCE" "$TRANSLATOR_SOURCE"; do
  [[ -f "$path" ]] || { echo "ERROR: missing $path" >&2; exit 2; }
done
[[ -d "$BUNDLE" ]] || { echo "ERROR: missing $BUNDLE" >&2; exit 2; }

python3 - "$REASONING_SOURCE" "$TRANSLATOR_SOURCE" "$STAMP" <<'PY'
import re
import sys
from pathlib import Path

reasoning, translator, stamp = map(Path, sys.argv[1:])

src = reasoning.read_text(encoding="utf-8")
old = '  if (hasExplicitReasoningField(body)) return body;\n  return { ...body, reasoning_effort: "max" };'
if old in src:
    reasoning.with_name(reasoning.name + "." + str(stamp) + ".bak").write_text(src, encoding="utf-8")
    reasoning.write_text(src.replace(old, '  return { ...body, reasoning_effort: "max" };', 1), encoding="utf-8")
    print("source: Gemini policy now overrides lower explicit effort")
elif 'if (provider !== "gemini") return body;' in src and 'return { ...body, reasoning_effort: "max" };' in src:
    print("source: Gemini policy already forced")
else:
    raise SystemExit("ERROR: unsupported defaultReasoningEffort.ts layout")

src = translator.read_text(encoding="utf-8")
start = src.find("  // OmniRoute's Gemini policy")
anchor = src.find("\n\n  const result: GeminiRequest = {", start)
if start < 0 or anchor < 0:
    raise SystemExit("ERROR: unsupported openai-to-gemini.ts layout")
replacement = '  // Provider-scoped policy: Gemini always receives max before translation.\n  body = { ...body, reasoning_effort: "max" };'
block = src[start:anchor]
if block != replacement:
    translator.with_name(translator.name + "." + str(stamp) + ".bak").write_text(src, encoding="utf-8")
    translator.write_text(src[:start] + replacement + src[anchor:], encoding="utf-8")
    print("source: translator now forces Gemini max")
else:
    print("source: translator already forces Gemini max")
PY

python3 - "$BUNDLE" "$STAMP" <<'PY'
import sys
from pathlib import Path

root, stamp = Path(sys.argv[1]), sys.argv[2]
old = 'e.startsWith("gemma-4"));else{if(t.reasoning_effort===undefined&&t.reasoning===undefined&&t.thinking===undefined&&!(t.output_config&&typeof t.output_config==="object"&&t.output_config.effort!==undefined))t={...t,reasoning_effort:"max"};if(t.reasoning_effort){'
new = 'e.startsWith("gemma-4"));else{t={...t,reasoning_effort:"max"};if(t.reasoning_effort){'
already = 'e.startsWith("gemma-4"));else{t={...t,reasoning_effort:"max"};if(t.reasoning_effort){'
patched = 0
ready = 0
for path in sorted(root.glob("*.js")):
    src = path.read_text(encoding="utf-8", errors="ignore")
    count = src.count(old)
    if count:
        path.with_name(path.name + "." + stamp + ".bak").write_text(src, encoding="utf-8")
        path.write_text(src.replace(old, new), encoding="utf-8")
        patched += count
    ready += src.count(already)
if patched + ready != 6:
    raise SystemExit(f"ERROR: expected 6 Gemini translator copies, found {patched} new + {ready} ready")
print(f"bundle: {patched} patched, {ready} already ready")
PY

if systemctl --user is-active --quiet omniroute.service 2>/dev/null; then
  systemctl --user restart omniroute.service
  echo "OmniRoute restarted"
fi

echo "Gemini max policy installed"
