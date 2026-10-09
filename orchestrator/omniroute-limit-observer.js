/**
 * OmniRoute limit observer for the Orquestador preset.
 *
 * OmniRoute already records request metadata in SQLite, but the raw history is
 * hard to scan when a free provider starts returning 429/404/5xx. This plugin
 * keeps a small JSONL audit stream and a durable per-model quarantine ledger.
 * It never retries a failed model itself: the companion quarantine controller
 * handles combo admission, while this file preserves the evidence needed to
 * understand the otherwise undocumented limits.
 *
 * @module omniroute-limit-observer
 */

import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import os from 'node:os'
import path from 'node:path'

export const name = 'omniroute-limit-observer'
export const inject = ['tools']

const HOME = os.homedir()
const DB_FILE = path.join(HOME, '.omniroute', 'storage.sqlite')
const LOG_DIR = path.join(HOME, '.dsh', 'logs')
const EVENT_LOG = path.join(LOG_DIR, 'omniroute-limit-events.jsonl')
const STATE_FILE = path.join(HOME, '.dsh', '.agent-presets', 'orchestrator', '.omniroute-limit-state.json')
const COOLDOWN_MS = 5 * 60 * 60 * 1000
const MAX_ROWS_PER_TICK = 500
const TARGET_PROVIDERS = new Set(['openrouter', 'gemini', 'ocwrap'])
const RELEVANT_TOOLS = new Set(['omniroute_code'])
const STARTED_JOB_RE = /started background subagent job (subagent-\d+)/
const CODE_MODELS = [
  'openrouter:openrouter/nex-agi/nex-n2.5-pro:free',
  'openrouter:cohere/north-mini-code:free',
  'gemini:gemini/gemini-flash-lite-latest',
  'ocwrap:ocwrap/big-pickle',
  'ocwrap:ocwrap/longcat-2.5-preview-free',
]

let DatabaseSync
try {
  ({ DatabaseSync } = await import('node:sqlite'))
} catch {
  // DSH normally runs on Node 24. The warning below makes an older runtime
  // visible instead of silently pretending that the observer is active.
}

function readJson(file, fallback) {
  try {
    return JSON.parse(readFileSync(file, 'utf8'))
  } catch {
    return fallback
  }
}

function resultText(result) {
  if (result === undefined || result === null) return ''
  if (Array.isArray(result.content)) {
    return result.content
      .map((block) => (block?.type === 'text' ? block.text : ''))
      .join('\n')
  }
  if (typeof result.value === 'string') return result.value
  return ''
}

function compact(value, max = 500) {
  return String(value ?? '').replace(/\s+/g, ' ').slice(0, max)
}

function classify(row) {
  const status = Number(row.status) || 0
  const text = `${row.error_summary ?? ''} ${row.error_type ?? ''}`
  if (status === 429 || /rate.?limit|too many requests|quota|daily limit|free-models-per-day/i.test(text)) {
    return 'rate_limit_or_quota'
  }
  if (status === 404 || /model.*not found|not found|unavailable/i.test(text)) return 'model_unavailable'
  if (/timeout|timed out|deadline/i.test(text)) return 'timeout'
  if (status >= 500 || /upstream|server error|bad gateway/i.test(text)) return 'provider_error'
  if (status >= 400) return 'request_error'
  return null
}

function providerOf(row) {
  const requested = String(row.requested_model ?? '').toLowerCase()
  if (requested.startsWith('ocwrap/')) return 'ocwrap'
  if (requested.startsWith('gemini/')) return 'gemini'
  if (requested.startsWith('openrouter/')) return 'openrouter'
  return String(row.provider ?? '').toLowerCase()
}

function qualityFailure(row) {
  if (Number(row.status) !== 200 || Number(row.has_response_body) !== 1 || !row.artifact_relpath) return null
  const file = path.join(HOME, '.omniroute', 'call_logs', row.artifact_relpath)
  if (!existsSync(file)) return null
  try {
    const payload = JSON.parse(readFileSync(file, 'utf8'))
    const choice = payload.responseBody?.choices?.[0]
    if (choice) {
      const message = choice.message ?? choice.delta ?? {}
      const content = typeof message.content === 'string' ? message.content.trim() : ''
      if (!content && !Array.isArray(message.tool_calls)) return 'quality_failure: empty assistant content'
    }
    const candidate = payload.responseBody?.candidates?.[0]
    if (candidate) {
      const visible = (candidate.content?.parts ?? []).some((part) => part?.text && part.thought !== true)
      if (!visible) return 'quality_failure: no visible assistant content'
    }
  } catch {
    return null
  }
  return null
}

function isRelevant(row) {
  const provider = providerOf(row)
  const combo = String(row.combo_name ?? '').toLowerCase()
  return combo === 'code' || combo === 'free' || TARGET_PROVIDERS.has(provider)
}

function ensureState(state) {
  if (!state || typeof state !== 'object') state = {}
  if (!state.models || typeof state.models !== 'object') state.models = {}
  if (!Number.isFinite(state.lastRowid)) state.lastRowid = 0
  return state
}

/** @param {object} ctx */
export function apply(ctx) {
  const state = ensureState(readJson(STATE_FILE, {}))
  const jobs = new Map()
  const guarded = new WeakSet()
  let collecting = false
  let warnedNoSqlite = false

  function persist() {
    try {
      mkdirSync(path.dirname(STATE_FILE), { recursive: true })
      writeFileSync(STATE_FILE, `${JSON.stringify(state, null, 2)}\n`)
    } catch (error) {
      ctx.logger?.warn?.(`omniroute-limit-observer: no pude guardar estado: ${error.message}`)
    }
  }

  function event(entry) {
    try {
      mkdirSync(LOG_DIR, { recursive: true })
      appendFileSync(EVENT_LOG, `${JSON.stringify({ at: new Date().toISOString(), ...entry })}\n`)
    } catch (error) {
      ctx.logger?.warn?.(`omniroute-limit-observer: no pude guardar log: ${error.message}`)
    }
  }

  function quarantine(row, reason) {
    const provider = providerOf(row) || 'unknown'
    const model = String(row.model ?? row.requested_model ?? 'unknown')
    const key = `${provider}:${model}`
    const now = Date.now()
    const previous = state.models[key]
    const active = previous && Number(previous.cooldownUntil) > now
    const cooldownUntil = active ? previous.cooldownUntil : now + COOLDOWN_MS
    state.models[key] = {
      provider,
      model,
      firstErrorAt: previous?.firstErrorAt ?? new Date(now).toISOString(),
      lastErrorAt: new Date(now).toISOString(),
      errorCount: Number(previous?.errorCount ?? 0) + 1,
      reason,
      cooldownUntil,
      cooldownUntilIso: new Date(cooldownUntil).toISOString(),
    }
    event({
      action: active ? 'error_during_cooldown' : 'quarantine',
      provider,
      model,
      status: row.status ?? null,
      reason,
      cooldownUntil: new Date(cooldownUntil).toISOString(),
      correlationId: row.correlation_id ?? null,
    })
    if (!active) {
      ctx.logger?.warn?.(
        `omniroute-limit-observer: ${provider}/${model} en cuarentena por 5h (${reason}); hasta ${new Date(cooldownUntil).toISOString()}`,
      )
    }
  }

  function collect() {
    if (collecting || !DatabaseSync) {
      if (!DatabaseSync && !warnedNoSqlite) {
        warnedNoSqlite = true
        ctx.logger?.warn?.('omniroute-limit-observer: este Node no expone node:sqlite; no habrá telemetría de límites')
      }
      return
    }
    collecting = true
    let db
    try {
      db = new DatabaseSync(DB_FILE, { readOnly: true })
      const rows = db.prepare(`
        SELECT rowid, timestamp, status, model, requested_model, provider,
               combo_name, error_summary, error_type, correlation_id,
               duration, tokens_in, tokens_out, tokens_reasoning,
               artifact_relpath, has_response_body
        FROM call_logs
        WHERE rowid > ?
        ORDER BY rowid ASC
        LIMIT ${MAX_ROWS_PER_TICK}
      `).all(state.lastRowid)
      for (const row of rows) {
        state.lastRowid = Number(row.rowid)
        if (!isRelevant(row)) continue
        const reason = classify(row) ?? qualityFailure(row)
        const entry = {
          action: reason ? 'observed_error' : 'success',
          timestamp: row.timestamp ?? null,
          provider: providerOf(row),
          model: row.model ?? row.requested_model ?? null,
          requestedModel: row.requested_model ?? null,
          combo: row.combo_name ?? null,
          status: row.status ?? null,
          reason,
          durationMs: row.duration ?? null,
          tokensIn: row.tokens_in ?? null,
          tokensOut: row.tokens_out ?? null,
          tokensReasoning: row.tokens_reasoning ?? null,
          correlationId: row.correlation_id ?? null,
          error: compact(row.error_summary),
        }
        event(entry)
        if (reason) quarantine(row, reason)
      }
      persist()
    } catch (error) {
      if (!String(error.message).includes('no such table')) {
        ctx.logger?.warn?.(`omniroute-limit-observer: lectura de SQLite falló: ${error.message}`)
      }
    } finally {
      try { db?.close() } catch {}
      collecting = false
    }
  }

  function activeQuarantine() {
    const controllerState = readJson(path.join(HOME, '.omniroute', 'quarantine-state.json'), {})
    return controllerState.models && typeof controllerState.models === 'object' ? controllerState.models : {}
  }

  function guardAgent(agent) {
    if (!agent || guarded.has(agent)) return
    guarded.add(agent)
    try {
      agent.ctx.inject(['tools'], (scope) => {
        scope.tools.guard((execution) => {
          const quarantines = activeQuarantine()
          const now = Date.now()
          if (execution.name === 'omniroute_code') {
            const allCooling = CODE_MODELS.every((modelKey) => Number(quarantines[modelKey]?.cooldownUntil) > now)
            if (allCooling) return 'Todos los modelos del combo code están en cuarentena. Espera a que venza una cuarentena de 5 horas; no fuerces reintentos.'
          }
          return undefined
        })
      })
    } catch (error) {
      ctx.logger?.warn?.(`omniroute-limit-observer: no pude instalar guard: ${error.message}`)
    }
  }

  ctx.on?.('tools/result', (_call, result) => {
    const text = resultText(result)
    const tool = _call?.toolName ?? _call?.name
    if (RELEVANT_TOOLS.has(tool)) {
      const match = text.match(STARTED_JOB_RE)
      if (match) jobs.set(match[1], { tool, at: Date.now() })
      collect()
      return
    }
    if (text.includes('subagent-')) collect()
  })

  ctx.on?.('agent/created', ({ agent }) => guardAgent(agent))
  ctx.on?.('agent/session-start', ({ agent }) => guardAgent(agent))
  ctx.on?.('agent/pre-step', ({ agent }, next) => {
    guardAgent(agent)
    return next()
  })

  // Catch calls that finish without a matching DSH tool result and give the
  // observer a bounded polling cadence while a preset session is alive.
  const timer = setInterval(collect, 15_000)
  timer.unref?.()
  collect()
  persist()
}
