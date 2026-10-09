# DeepSeek Harness Orchestrator + OmniRoute

Public replication bundle for the read-only Orchestrator preset and its local
OmniRoute `code` combo. This package intentionally contains no API keys,
tokens, `.env` files, SQLite databases, call logs, quarantine state, or session
prompts.

## Architecture

```text
DeepSeek Harness (read-only)
  -> omniroute_code (in-process, the only delegation tool)
  -> OpenCode/Zen
  -> OpenRouter free models
  -> Gemini Flash Lite (last fallback, reasoning max)
```

The combo is priority-ordered:

1. `ocwrap/big-pickle`
2. `ocwrap/longcat-2.5-preview-free`
3. OpenRouter `cohere/north-mini-code:free`
4. OpenRouter `nex-agi/nex-n2.5-pro:free`
5. Gemini `gemini/gemini-flash-lite-latest`

When the resolved provider is Gemini, the included OmniRoute patch forces
`reasoning_effort: max` before the OpenAI-to-Gemini translation. The patch is
provider-scoped and does not send `max` to OpenCode or OpenRouter.

## Prerequisites

- DeepSeek Harness with support for local preset plugins and the in-process
  subagent provider.
- OmniRoute installed globally and available as `omniroute`.
- `ocwrap` configured locally for the OpenCode/Zen lane.
- OpenRouter and Gemini credentials configured in the target machine's private
  OmniRoute environment. Do not put them in this repository.
- Node.js and Python 3.

## Install

1. Copy `orchestrator/*` to:
   `~/.dsh/.agent-presets/orchestrator/`
2. Merge `omniroute-settings.example.yaml` into `~/.dsh/settings.yaml`.
3. Run:

   ```bash
   bash scripts/configure-code-combo.sh
   bash scripts/patch-gemini-max.sh
   bash scripts/verify-installation.sh
   ```

4. Select the `Orquestador` preset in DeepSeek Harness and start a new
   session.

The combo script replaces only the `code` combo. It leaves OmniRoute's
`free` combo unchanged. The patch script creates timestamped backups beside
the installed OmniRoute bundle before editing release files.

## Private environment

Set credentials outside this repository, for example in the private files
used by the local services:

```text
OPENCODE_API_KEY=<local secret>
OPENROUTER_API_KEY=<local secret>
GOOGLE_API_KEY=<local secret>
```

The exact credential names may vary with the installed OmniRoute provider
configuration. The replica only references provider names and environment
variable names; it never stores their values.

## Verification and telemetry

```bash
omniroute combo list --json
systemctl --user is-active omniroute.service
```

Runtime evidence stays outside the repository:

- `~/.omniroute/storage.sqlite`
- `~/.omniroute/call_logs/`
- `~/.dsh/logs/omniroute-limit-events.jsonl`
- `~/.dsh/.agent-presets/orchestrator/.omniroute-limit-state.json`

The observer records provider/model failures and the local quarantine policy
keeps a failed model out for at least five hours. Do not publish these files:
they can contain prompts, response content, account metadata, or local paths.

## Release note

The Gemini patch targets the current OmniRoute release bundle layout. Re-run
`scripts/patch-gemini-max.sh` after upgrading OmniRoute and inspect its
verification output before using the preset.
