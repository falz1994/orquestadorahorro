/**
 * Orchestrator job watchdog.
 *
 * Evidence-based liveness watchdog for the Orquestador preset. The preset's
 * subagents currently run in-process through OmniRoute. DSH reports them as
 * `running` while a child works or waits on a provider, so a status alone is not
 * evidence of progress. The orchestrator used to wait 30-60 min on those jobs
 * because "running" looked like "still working".
 *
 * The first two generations of this plugin killed on TIMERS fed by inputs that
 * are silent while a child legitimately works (file mtimes of prompt-derived
 * paths, plus non-empty `job_output` polls). That cost real work: healthy
 * children died at 6 min (preflight false positive) and at 30 min (paths
 * resolved against the wrong directory) while they were writing their reports.
 * Audited 2026-09-23; both root causes are fixed, and the verdict no longer
 * rests on age.
 *
 * What the plugin watches now, per job — a signal in ANY channel resets the
 * silence clock:
 *
 *   1. ESCRITURAS en los archivos objetivo que el prompt nombró (mtime avanzó).
 *   2. PROCESO del hijo (`/proc/<pid>`): sigue vivo, y sus contadores de CPU
 *      (utime+stime) o de I/O (read_bytes+write_bytes) avanzaron desde el tick
 *      anterior. Esto NO depende de que el hijo coopere ni de que emita texto.
 *   3. SALIDA del job: un `job_output` que trae texto. Un poll que responde
 *      "(no new output)" NO es evidencia de estancamiento: un hijo que está
 *      haciendo tool calls puede verse mudo por diseño.
 *   4. HERRAMIENTA EN VUELO: para hijos in-process, un `tool/call` sin su
 *      `tool/result` cuenta como vida hasta su deadline declarado. Esto evita
 *      confundir un test o render legítimo con un hijo colgado.
 *
 * Y el veredicto:
 *
 *   - `killSilenceMs` (default 45 min): mata sólo tras silencio TOTAL en todos
 *     los canales. Medido: un hijo sano puede estar 16.9 min sin ninguna señal
 *     mientras una llamada al modelo del tier gratuito está en vuelo, y hasta
 *     20.4 min entre dos escrituras.
 *   - `warnSilenceMs` (default 20 min): NO mata; avisa por inbox al orquestador
 *     (más `logger.warn` y una línea en `<cwd>/.dsh-tmp/watchdog-stalls.jsonl`)
 *     con la evidencia, y deja la decisión al agente.
 *   - Proceso ausente: si el hijo que ya habíamos visto desaparece, se cierra el
 *     job de inmediato. Eso es evidencia dura de muerte, no una sospecha.
 *   - `preflightWarnMs` / `preflightKillMs` (3 / 6 min): un preflight real
 *     ("Respondé exactamente: OK") contesta en segundos; si está mudo, falla
 *     rápido. El regex que lo detecta está anclado, así que un prompt de frame
 *     que menciona "el preflight del render" ya no cae acá.
 *   - `maxRuntimeMs` (default 75 min): techo absoluto.
 *
 * El motivo de cada kill o aviso viaja en el `reason`/mensaje y en el log de
 * stalls: un `killed` pelado, sin evidencia, fue justamente lo que hizo
 * indiagnosticable la generación anterior.
 *
 * @module orchestrator-job-watchdog
 */

import { execFile } from 'node:child_process'
import { randomUUID } from 'node:crypto'
import { existsSync } from 'node:fs'
import { appendFile, mkdir, readdir, readFile, readlink, stat } from 'node:fs/promises'
import { homedir } from 'node:os'
import path from 'node:path'
import { promisify } from 'node:util'

export const name = 'orchestrator-job-watchdog'
export const inject = ['jobs', 'tools']

/**
 * Silence thresholds, measured on 2026-09-23 against the children's own logs.
 *
 * The previous design killed on TIMERS fed by two blind inputs: file mtimes of
 * prompt-derived paths, and non-empty `job_output` polls. Both stay silent while
 * a child legitimately works, so healthy children died at 6 min (a preflight
 * false positive) and at 30 min (paths resolved against the wrong directory).
 *
 * What the measurement says:
 *   - a healthy child can go SILENT IN EVERY CHANNEL for 16.9 min while one
 *     free-tier model call is in flight (`"llm runtime selected"` followed by
 *     16.9 min of nothing, twice in the audited sample);
 *   - up to 20.4 min can pass between two file writes of a healthy child;
 *   - a genuinely hung child sat silent for 228 min until it was cancelled.
 *
 * So the watchdog no longer decides by age. It WARNs (never kills) after 20 min
 * of total silence and kills only after 45 min of total silence, or immediately
 * when the child process is gone — that is hard evidence of death, not a guess
 * about stagnation. Override with `DSH_JOB_WATCHDOG_WARN_SILENCE_MS` and
 * `DSH_JOB_WATCHDOG_KILL_SILENCE_MS` (the host must restart to pick them up).
 */
const DEFAULT_WARN_SILENCE_MS = 20 * 60 * 1000
const DEFAULT_KILL_SILENCE_MS = 45 * 60 * 1000
/** A real preflight answers in seconds; if it is silent at all, fail fast. */
const DEFAULT_PREFLIGHT_WARN_MS = 3 * 60 * 1000
const DEFAULT_PREFLIGHT_KILL_MS = 6 * 60 * 1000
const DEFAULT_MAX_RUNTIME_MS = 75 * 60 * 1000
const DEFAULT_POLL_INTERVAL_MS = 30 * 1000
/** Never judge a missing child process before the job had time to spawn it. */
const PROCESS_GRACE_MS = 2 * 60 * 1000
/** How far a child's start time may sit from the job's to still be its child. */
const CHILD_MATCH_WINDOW_MS = 3 * 60 * 1000
/** Retry window for finding the child process while it has not been seen yet. */
const DISCOVERY_RETRY_MS = 15 * 1000
/**
 * Un hijo que muere en menos de esto y sin ninguna salida no se estancó: falló
 * al arrancar (ruta de modelo sin credencial, provider mal configurado, prompt
 * rechazado). El watchdog rescata el motivo del transcript del hijo y se lo
 * entrega al orquestador, porque el job sólo dice `failed, error`.
 */
const DEFAULT_FAST_FAIL_MS = 10 * 1000
/** Recorte del diagnóstico que se le pasa al orquestador. */
const DIAG_MAX_CHARS = 700
/** Presupuesto para una herramienta sin timeout explícito. */
const DEFAULT_INFLIGHT_BUDGET_MS = 5 * 60 * 1000
/** Margen para la entrega del resultado después de su deadline declarado. */
const INFLIGHT_GRACE_MS = 30 * 1000
/** Cada cuánto reintentar resolver la sesión hija de un job in-process. */
const CHILD_LOOKUP_RETRY_MS = 2 * 60 * 1000
/** Raíz de los transcripts de sesión de DSH. */
const SESSIONS_DIR = path.join(homedir(), '.dsh', 'sessions')
const execFileAsync = promisify(execFile)

/** Linux USER_HZ: /proc/<pid>/stat times are counted in these ticks. */
const CLOCK_TICKS_PER_SECOND = 100

const TARGET_EXTENSIONS = [
  'html', 'htm', 'md', 'markdown', 'json', 'jsonl', 'txt',
  'py', 'sh', 'bash', 'js', 'mjs', 'cjs', 'ts', 'tsx', 'jsx',
  'css', 'scss', 'yml', 'yaml', 'toml', 'ini', 'cfg', 'conf',
]

const TARGET_PATH_RE = new RegExp(
  String.raw`(?:[A-Za-z0-9_.-]+\/)*[A-Za-z0-9_.-]+\.(?:${TARGET_EXTENSIONS.join('|')})`,
  'g',
)

/**
 * The orchestrator's own `description` is a label it writes on purpose, so a
 * loose match there is safe.
 */
const PREFLIGHT_LABEL_RE = /preflight|probe|availability|respond[eé] exactamente:\s*ok/i
/**
 * The PROMPT needs an ANCHORED match. A frame-authoring prompt legitimately
 * carries the composition contract line "si falta algo, el preflight del render
 * falla"; the loose regex matched that body text and classified every frame job
 * as a preflight, which then killed it on the fast preflight clock mid-work
 * (audited 2026-09-23: subagents 4-8, killed at 6m07s-6m24s while writing
 * frames). Anchoring to a line start keeps real preflights detectable:
 * "PREFLIGHT ...", "# preflight", "Respondé exactamente: OK".
 */
const PREFLIGHT_PROMPT_RE = /^\s*(?:#+\s*)?(?:preflight|probe)\b|respond[eé] exactamente:\s*ok/im
const STARTED_JOB_RE = /started background subagent job (subagent-\d+)/

function envMs(name, fallback) {
  const raw = process.env[name]
  if (raw === undefined || raw === '') return fallback
  const value = Number(raw)
  return Number.isFinite(value) && value > 0 ? value : fallback
}

function resultText(result) {
  if (result === undefined || result === null) return ''
  if (Array.isArray(result.content)) {
    return result.content
      .map((block) => (block && block.type === 'text' ? block.text : ''))
      .join('\n')
  }
  if (typeof result.value === 'string') return result.value
  return ''
}

/**
 * Devuelve la última herramienta del hijo que sigue en vuelo y todavía está
 * dentro de su deadline. Pasado ese presupuesto deja de ser señal de vida: un
 * tool realmente colgado no puede mantener un job vivo indefinidamente.
 */
export function inFlightTool(events, now) {
  if (!Array.isArray(events) || events.length === 0) return undefined
  const open = new Map()
  for (const event of events) {
    const data = event?.data
    if (event?.type === 'tool/call') {
      const id = typeof data?.callId === 'string' ? data.callId : undefined
      if (id === undefined) continue
      open.set(id, {
        name: typeof data?.name === 'string' ? data.name : 'herramienta',
        args: data?.arguments,
        at: typeof event.time === 'number' ? event.time : now,
      })
    } else if (event?.type === 'tool/result') {
      const id = typeof data?.callId === 'string'
        ? data.callId
        : (typeof data?.message?.source?.callId === 'string' ? data.message.source.callId : undefined)
      if (id !== undefined) open.delete(id)
    }
  }
  let latest
  for (const entry of open.values()) {
    if (latest === undefined || entry.at > latest.at) latest = entry
  }
  if (latest === undefined) return undefined
  const args = latest.args !== null && typeof latest.args === 'object' ? latest.args : {}
  const declared = typeof args.timeoutMs === 'number' && args.timeoutMs > 0 ? args.timeoutMs : 0
  const budget = (declared > 0 ? declared : DEFAULT_INFLIGHT_BUDGET_MS) + INFLIGHT_GRACE_MS
  if (now - latest.at > budget) return undefined
  return { name: latest.name, startedAt: latest.at, budget }
}

/** True when `candidate` lives inside the session workspace `cwd`. */
function insideWorkspace(candidate, cwd) {
  const relative = path.relative(cwd, candidate)
  return relative !== '' && !relative.startsWith('..') && !path.isAbsolute(relative)
}

/**
 * Absolute directories named in the prompt, most specific first.
 *
 * The orchestrator pastes the PROJECT path absolutely ("…/Gradient
 * Descente/.moe-build") while every file name in the same prompt stays relative
 * to it. Resolving those names against the session `cwd` alone (the project's
 * PARENT) produced paths that never exist, so `changedOnce` never became true
 * and the watchdog killed healthy children on the no-first-change grace
 * (audited 2026-09-23: audit jobs 5-7, killed at 30m14s while writing their
 * reports). Note the space in "Gradient Descente": the path regex cannot span
 * it, so an absolute path must be captured here, whole, before matching.
 */
function promptBaseDirs(prompt, cwd) {
  const bases = new Set([cwd])
  for (const match of String(prompt || '').matchAll(/(?:^|[\s"'`(=:,>\[\]()])(\/[^\n"'`()\[\]{},;]+)/g)) {
    const raw = match[1].replace(/[.,;:]+$/, '')
    if (!/^\/[A-Za-z0-9_.-]/.test(raw)) continue
    const normalized = path.normalize(raw)
    bases.add(normalized)
    // The prompt may name a file; its directory is the useful base.
    bases.add(path.dirname(normalized))
  }
  // Longest first: the most specific directory wins when several prefixes match.
  return [...bases].sort((a, b) => b.length - a.length)
}

function normalizePromptPath(value, cwd, bases) {
  const cleaned = String(value || '').replace(/^\.\//, '')
  if (!cleaned) return undefined
  if (path.isAbsolute(cleaned)) {
    const absolute = path.normalize(cleaned)
    return insideWorkspace(absolute, cwd) ? absolute : undefined
  }
  let fallback
  for (const base of bases) {
    const absolute = path.resolve(base, cleaned)
    if (!insideWorkspace(absolute, cwd)) continue
    // A file that already exists is the best evidence of which base is real.
    if (existsSync(absolute)) return absolute
    // Otherwise prefer a base whose directory exists: the child is about to
    // create the file there (report paths usually do not exist at job start).
    if (fallback === undefined && existsSync(path.dirname(absolute))) fallback = absolute
  }
  return fallback
}

function extractTargetPaths(prompt, cwd) {
  const found = new Set()
  const bases = promptBaseDirs(prompt, cwd)
  for (const match of String(prompt || '').matchAll(TARGET_PATH_RE)) {
    const absolute = normalizePromptPath(match[0], cwd, bases)
    if (absolute) found.add(absolute)
  }
  return [...found]
}

/**
 * Extrae el motivo de fallo de los eventos de una sesión hija.
 *
 * Los dos sitios donde DSH deja el error: el `turn/end` con
 * `reason.kind === 'error'`, y el chunk `finish` del `assistant/attempt`
 * (`reason.failure.message`). Se recorre al revés: el último gana.
 *
 * @param events - eventos de la sesión hija, en orden.
 * @returns el mensaje de error, o `undefined` si no hay ninguno.
 */
function failureFromEvents(events) {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    const event = events[index]
    const data = event?.data
    if (event?.type === 'turn/end' && data?.reason?.kind === 'error') {
      const message = data.reason.error?.message
      if (typeof message === 'string' && message !== '') return message
    }
    if (event?.type === 'assistant/attempt' && Array.isArray(data?.stream)) {
      for (const entry of data.stream) {
        const chunk = entry?.chunk
        if (chunk?.type !== 'finish' || chunk?.reason?.kind !== 'error') continue
        const message = chunk.reason.failure?.message
        if (typeof message === 'string' && message !== '') return message
      }
    }
  }
  return undefined
}

/**
 * Último recurso cuando el agente hijo ya no está residente: buscar su
 * transcript en `~/.dsh/sessions/<proyecto>/<childId>/session.v3.jsonl.zstd` y
 * descomprimirlo con `zstd -dc`. Sólo se leen las últimas líneas.
 *
 * @param childId - id de la sesión hija.
 * @returns el mensaje de error, o `undefined`.
 */
async function readChildFailureFile(childId) {
  let projectDirs
  try {
    projectDirs = await readdir(SESSIONS_DIR)
  } catch {
    return undefined
  }
  for (const projectDir of projectDirs) {
    const file = path.join(SESSIONS_DIR, projectDir, childId, 'session.v3.jsonl.zstd')
    if (!existsSync(file)) continue
    try {
      const { stdout } = await execFileAsync('zstd', ['-dc', file], {
        maxBuffer: 8 * 1024 * 1024,
        timeout: 15 * 1000,
      })
      const events = []
      for (const line of String(stdout).trim().split('\n').slice(-120)) {
        try {
          events.push(JSON.parse(line))
        } catch {
          // Una línea truncada al final no invalida el resto.
        }
      }
      return failureFromEvents(events)
    } catch {
      return undefined
    }
  }
  return undefined
}

function ownerCwd(owner) {
  return owner?.session?.header?.cwd || process.cwd()
}

/**
 * Parse `/proc/<pid>/stat`. The `comm` field may contain spaces and parentheses,
 * so every index is anchored off the LAST ')'.
 */
function parseProcStat(raw) {
  const close = raw.lastIndexOf(')')
  if (close < 0) return undefined
  const fields = raw.slice(close + 2).trim().split(/\s+/)
  return {
    state: fields[0],
    ppid: Number(fields[1]),
    // utime (field 14) + stime (field 15) → indices 11 and 12 after the cut.
    cpuTicks: Number(fields[11]) + Number(fields[12]),
    // starttime (field 22) → index 19.
    startTicks: Number(fields[19]),
  }
}

let cachedBootTimeMs
async function systemBootTimeMs() {
  if (cachedBootTimeMs !== undefined) return cachedBootTimeMs
  try {
    const raw = await readFile('/proc/stat', 'utf8')
    const match = /^btime\s+(\d+)$/m.exec(raw)
    cachedBootTimeMs = match === null ? 0 : Number(match[1]) * 1000
  } catch {
    cachedBootTimeMs = 0
  }
  return cachedBootTimeMs
}

/** Live facts about one child process; `undefined` once the process is gone. */
async function readProcStat(pid) {
  try {
    const raw = await readFile(`/proc/${pid}/stat`, 'utf8')
    return parseProcStat(raw)
  } catch {
    return undefined
  }
}

/** Per-process disk I/O; `undefined` when the kernel or permissions hide it. */
async function readProcIo(pid) {
  try {
    const raw = await readFile(`/proc/${pid}/io`, 'utf8')
    const read = /^read_bytes:\s*(\d+)$/m.exec(raw)
    const write = /^write_bytes:\s*(\d+)$/m.exec(raw)
    if (read === null && write === null) return undefined
    return Number(read?.[1] ?? 0) + Number(write?.[1] ?? 0)
  } catch {
    return undefined
  }
}

/**
 * Find the `opencode acp` process that belongs to this job: a direct child of
 * this host process, started around the job's own start, not already claimed by
 * another record. The bridge spawns one child per job, so its CPU/IO counters
 * are the job's real heartbeat — and they are observable without the child
 * cooperating.
 */
async function discoverChildPid(record, claimed) {
  let entries
  try {
    entries = await readdir('/proc')
  } catch {
    return undefined
  }
  const boot = await systemBootTimeMs()
  let best
  for (const name of entries) {
    if (!/^\d+$/.test(name)) continue
    const pid = Number(name)
    if (pid === process.pid || claimed.has(pid)) continue
    // stat first (cheap), then cmdline, and only read I/O for a real candidate:
    // /proc has hundreds of entries and this runs on the orchestrator's thread.
    const info = await readProcStat(pid)
    if (info === undefined || info.ppid !== process.pid) continue
    let cmdline
    try {
      cmdline = (await readFile(`/proc/${pid}/cmdline`, 'utf8')).replace(/\0/g, ' ').trim()
    } catch {
      continue
    }
    if (!/opencode/i.test(cmdline) || !/\bacp\b/i.test(cmdline)) continue
    const startedAt = boot + (info.startTicks / CLOCK_TICKS_PER_SECOND) * 1000
    const delta = Math.abs(startedAt - record.startedAt)
    if (delta > CHILD_MATCH_WINDOW_MS) continue
    let cwd
    try {
      cwd = await readlink(`/proc/${pid}/cwd`)
    } catch {
      cwd = undefined
    }
    const score = (cwd === record.cwd ? 0 : 1) * 1e7 + delta
    if (best === undefined || score < best.score) best = { pid, score }
  }
  return best?.pid
}

/**
 * Append one line to `<cwd>/.dsh-tmp/watchdog-stalls.jsonl`: a stall has to
 * leave a trail someone can read afterwards, because the job status alone never
 * carried the reason for a kill.
 */
async function appendStallLog(record, line) {
  try {
    const dir = path.join(record.cwd, '.dsh-tmp')
    await mkdir(dir, { recursive: true })
    await appendFile(
      path.join(dir, 'watchdog-stalls.jsonl'),
      `${JSON.stringify({ time: new Date().toISOString(), job: record.label, line })}\n`,
    )
  } catch {
    // Best effort: a diagnostic trail must never break the tick.
  }
}

/**
 * @param {import('@deepseek-ai/cordis').Context} ctx - preset-scoped context.
 */
export function apply(ctx) {
  const warnSilenceMs = envMs('DSH_JOB_WATCHDOG_WARN_SILENCE_MS', DEFAULT_WARN_SILENCE_MS)
  const killSilenceMs = envMs('DSH_JOB_WATCHDOG_KILL_SILENCE_MS', DEFAULT_KILL_SILENCE_MS)
  const preflightWarnMs = envMs('DSH_JOB_WATCHDOG_PREFLIGHT_WARN_MS', DEFAULT_PREFLIGHT_WARN_MS)
  const preflightKillMs = envMs('DSH_JOB_WATCHDOG_PREFLIGHT_MS', DEFAULT_PREFLIGHT_KILL_MS)
  const maxRuntimeMs = envMs('DSH_JOB_WATCHDOG_MAX_RUNTIME_MS', DEFAULT_MAX_RUNTIME_MS)
  const pollIntervalMs = envMs('DSH_JOB_WATCHDOG_POLL_MS', DEFAULT_POLL_INTERVAL_MS)
  const fastFailMs = envMs('DSH_JOB_WATCHDOG_FAST_FAIL_MS', DEFAULT_FAST_FAIL_MS)

  /**
   * @type {Map<string, {owner: object, startedAt: number, paths: string[], lastFileMtime: number, label: string, tool: string, cwd: string, preflight: boolean, killing: boolean, pid?: number, sawProcess: boolean, cpuTicks?: number, ioBytes?: number, lastSignalAt: number, lastSignalKind: string, warnedAt?: number, childId?: string, childLookupAt?: number}>}
   */
  const records = new Map()
  let ticking = false

  /** A fresh record: every signal clock starts at the job's own start. */
  function newRecord(owner, startedAt, cwd) {
    return {
      owner,
      startedAt,
      paths: [],
      lastFileMtime: 0,
      label: '',
      tool: '',
      cwd,
      preflight: false,
      killing: false,
      pid: undefined,
      sawProcess: false,
      cpuTicks: undefined,
      ioBytes: undefined,
      lastSignalAt: startedAt,
      lastSignalKind: 'arranque',
      warnedAt: undefined,
      nextDiscoveryAt: 0,
      childId: undefined,
      childLookupAt: undefined,
    }
  }

  /** Resolve and cache the child session for an in-process job. */
  async function childIdOf(record) {
    if (record.childId !== undefined) return record.childId
    const now = Date.now()
    if (record.childLookupAt !== undefined && now - record.childLookupAt < CHILD_LOOKUP_RETRY_MS) {
      return undefined
    }
    record.childLookupAt = now
    let children
    try {
      children = await ctx.subagents?.listChildren?.(record.owner?.session?.id)
    } catch {
      return undefined
    }
    if (!Array.isArray(children) || children.length === 0) return undefined
    const sameLabel = children.filter((child) => child.label === record.label)
    const pool = sameLabel.length > 0 ? sameLabel : children
    const id = pool[pool.length - 1]?.id
    if (typeof id !== 'string' || id === '') return undefined
    record.childId = id
    return id
  }

  /** Read a bounded liveness signal from an in-process child's own events. */
  async function childInFlight(record, now) {
    const childId = await childIdOf(record)
    if (childId === undefined) return undefined
    try {
      return inFlightTool(ctx.agents?.get?.(childId)?.session?.ownEvents?.(), now)
    } catch {
      return undefined
    }
  }

  function remember(owner, snapshot) {
    if (owner === undefined || owner === null || snapshot.kind !== 'subagent') return
    const existing = records.get(snapshot.id)
    if (existing !== undefined) {
      existing.owner = owner
      return
    }
    const record = newRecord(owner, snapshot.startedAt || Date.now(), ownerCwd(owner))
    record.label = snapshot.label || ''
    records.set(snapshot.id, record)
  }

  function syncOwner(owner) {
    if (owner === undefined || owner === null) return
    let snapshots
    try {
      snapshots = ctx.jobs.list(owner)
    } catch (error) {
      ctx.logger?.warn?.(`orchestrator-job-watchdog: no pude listar jobs: ${String(error)}`)
      return
    }
    for (const snapshot of snapshots) {
      if (snapshot.kind !== 'subagent') continue
      if (snapshot.status === 'running') {
        remember(owner, snapshot)
      } else {
        records.delete(snapshot.id)
      }
    }
  }

  ctx.on('tools/result', (exec, result) => {
    const text = resultText(result)

    // Non-consuming liveness signal: a `job_output` poll that actually carries
    // text proves the child is producing. A poll answering "(no new output)" is
    // NOT evidence of stagnation: a child running tool calls can look silent by
    // design, so the in-flight-tool signal below is also considered.
    if (exec?.name === 'job_output') {
      const pollArgs = exec.arguments && typeof exec.arguments === 'object' ? exec.arguments : {}
      const polledId = typeof pollArgs.job_id === 'string' ? pollArgs.job_id : undefined
      const polled = polledId === undefined ? undefined : records.get(polledId)
      const trimmed = text.trim()
      if (polled !== undefined && trimmed !== '' && !trimmed.startsWith('(no new output)')) {
        polled.lastSignalAt = Date.now()
        polled.lastSignalKind = 'salida del hijo'
      }
    }

    const match = STARTED_JOB_RE.exec(text)
    if (match === null) return
    const owner = exec.agent
    if (owner === undefined || owner === null) return

    const args = exec.arguments && typeof exec.arguments === 'object' ? exec.arguments : {}
    const prompt = typeof args.prompt === 'string' ? args.prompt : ''
    const cwd = ownerCwd(owner)
    const isPreflight = PREFLIGHT_LABEL_RE.test(String(args.description || ''))
      || PREFLIGHT_PROMPT_RE.test(prompt)

    const existing = records.get(match[1])
    const record = existing || newRecord(owner, Date.now(), cwd)
    record.owner = owner
    record.cwd = cwd
    if (typeof args.description === 'string' && args.description.length > 0) {
      record.label = args.description
    }
    if (typeof exec.name === 'string' && exec.name !== '') record.tool = exec.name
    record.preflight = isPreflight
    record.paths = extractTargetPaths(prompt, cwd)
    records.set(match[1], record)
  })

  ctx.effect(
    () => ctx.jobs.onJobsChanged((owner) => syncOwner(owner)),
    'orchestrator-job-watchdog: jobs-changed',
  )

  ctx.effect(
    () =>
      ctx.jobs.onJobDone((snapshot, owner) => {
        if (owner === undefined || owner === null) return
        if (snapshot.kind !== 'subagent') return
        if (snapshot.status !== 'failed') return
        const finishedAt = typeof snapshot.finishedAt === 'number' ? snapshot.finishedAt : Date.now()
        if (finishedAt - snapshot.startedAt > fastFailMs) return
        void rescueFastFailure(snapshot, owner).catch((error) => {
          ctx.logger?.warn?.(
            `orchestrator-job-watchdog: el rescate de diagnóstico falló: ${String(error)}`,
          )
        })
      }),
    'orchestrator-job-watchdog: fast-fail rescue',
  )

  /** One-line evidence string shared by the warning, the kill reason and the log. */
  function evidenceOf(record, now, proc) {
    const silence = now - record.lastSignalAt
    const minutes = (ms) => `${Math.round(ms / 60000)} min`
    const procText = proc === undefined
      ? 'proceso AUSENTE'
      : `proceso vivo pid ${record.pid} (estado ${proc.state}, CPU ${proc.cpuTicks} ticks, I/O ${record.ioBytes ?? '?'} bytes)`
    return `${procText}; última señal: ${record.lastSignalKind} hace ${minutes(silence)}; `
      + `${record.paths.length} archivos objetivo vigilados; job de ${minutes(now - record.startedAt)}`
  }

  function killJob(jobId, record, reason) {
    try {
      const outcome = ctx.jobs.kill(jobId, record.owner, reason)
      record.killing = true
      ctx.logger?.info?.(`orchestrator-job-watchdog: ${outcome} ${jobId} (${record.label}) — ${reason}`)
      void appendStallLog(record, `KILL (${outcome}): ${reason}`)
      if (outcome === 'already-finished') records.delete(jobId)
    } catch (error) {
      ctx.logger?.warn?.(`orchestrator-job-watchdog: no pude matar ${jobId}: ${String(error)}`)
      records.delete(jobId)
    }
  }

  /**
   * Entrega un aviso al orquestador por los TRES canales, para que no se pierda:
   * mensaje de plugin en su inbox (`next-step`, porque suele estar bloqueado en
   * `job_output(wait: true)`), `logger.warn` del host, y una línea en
   * `<cwd>/.dsh-tmp/watchdog-stalls.jsonl`.
   *
   * @param owner - agente dueño del job (el orquestador).
   * @param cwd - directorio de la sesión, para el archivo de stalls.
   * @param subject - una línea para el log y el archivo.
   * @param body - texto completo que recibe el orquestador.
   */
  function deliverNotice(owner, cwd, subject, body) {
    try {
      owner?.inbox?.append?.('next-step', {
        id: randomUUID(),
        role: 'user',
        content: [{ type: 'text', text: body }],
        source: { kind: 'plugin', plugin: 'orchestrator-job-watchdog' },
      })
    } catch (error) {
      ctx.logger?.warn?.(`orchestrator-job-watchdog: no pude avisar por inbox: ${String(error)}`)
    }
    ctx.logger?.warn?.(`orchestrator-job-watchdog: ${subject}`)
    void appendStallLog({ cwd, label: subject }, subject)
  }

  /**
   * Rescate de diagnóstico: un hijo que muere en segundos y sin salida no se
   * estancó, falló al arrancar — y el job sólo reporta `failed, error`, sin el
   * motivo. Se busca la sesión hija, se lee el error de sus eventos (o de su
   * transcript en disco) y se le entrega al orquestador con el aviso.
   *
   * @param snapshot - snapshot terminal del job.
   * @param owner - agente dueño del job.
   */
  async function rescueFastFailure(snapshot, owner) {
    const cwd = ownerCwd(owner)
    const label = typeof snapshot.label === 'string' ? snapshot.label : ''
    let childId
    try {
      const children = await ctx.subagents?.listChildren?.(owner.session.id)
      if (Array.isArray(children) && children.length > 0) {
        // El label del hijo es la `description` de la llamada: empareja exacto.
        const sameLabel = children.filter((child) => child.label === label)
        const pool = sameLabel.length > 0 ? sameLabel : children
        childId = pool[pool.length - 1]?.id
      }
    } catch (error) {
      ctx.logger?.warn?.(`orchestrator-job-watchdog: no pude listar hijos: ${String(error)}`)
    }

    let failure
    if (childId !== undefined) {
      const agent = ctx.agents?.get?.(childId)
      if (agent?.session !== undefined) {
        try {
          failure = failureFromEvents(agent.session.ownEvents?.() ?? [])
        } catch {
          // El hijo puede estar cerrándose: se cae al transcript en disco.
        }
      }
      if (failure === undefined) failure = await readChildFailureFile(childId)
    }

    const finishedAt = typeof snapshot.finishedAt === 'number' ? snapshot.finishedAt : Date.now()
    const seconds = Math.max(1, Math.round((finishedAt - snapshot.startedAt) / 1000))
    const detail = typeof snapshot.detail === 'string' && snapshot.detail !== '' ? ` (${snapshot.detail})` : ''
    const lines = [
      `⚠ watchdog: el job ${snapshot.id} ("${label}") murió a los ${seconds}s sin producir salida${detail}.`,
      failure === undefined
        ? 'No pude leer el motivo en el transcript del hijo.'
        : `Motivo del hijo: ${failure.slice(0, DIAG_MAX_CHARS)}`,
      childId === undefined ? '' : `Sesión del hijo: ${childId}`,
      'Es un fallo de ARRANQUE, no un estancamiento: arreglá la causa (credencial, ruta de modelo, prompt) antes de relanzar igual.',
    ]
    deliverNotice(
      owner,
      cwd,
      `fallo rápido ${snapshot.id} (${label}) — ${failure ?? 'sin motivo legible'}`,
      lines.filter((line) => line !== '').join('\n'),
    )
  }

  /**
   * Warn the orchestrator and DO NOT kill. The job keeps running: the point is
   * that a silent child may still be waiting on a slow provider call, and only
   * the agent (or the user) has the context to decide. Delivered three ways so
   * it cannot be missed: an inbox message on the owner's next step, a
   * `logger.warn`, and a line in `<cwd>/.dsh-tmp/watchdog-stalls.jsonl`.
   */
  function warnStall(jobId, record, now, proc, killMs) {
    const silence = now - record.lastSignalAt
    const evidence = evidenceOf(record, now, proc)
    const text = [
      `⚠ watchdog: ${jobId} ("${record.label}") lleva ${Math.round(silence / 60000)} min sin NINGUNA señal de vida.`,
      `Evidencia: ${evidence}.`,
      `NO lo maté: el umbral de kill es ${Math.round(killMs / 60000)} min de silencio total.`,
      `Decidí vos: esperarlo con job_output({ job_id: "${jobId}", wait: true }) o cerrarlo con job_kill.`,
    ].join('\n')
    deliverNotice(record.owner, record.cwd, `aviso ${jobId} (${record.label}) — ${evidence}`, text)
  }

  async function tick() {
    if (ticking) return
    ticking = true
    try {
      const now = Date.now()
      const claimed = new Set()
      for (const record of records.values()) {
        if (typeof record.pid === 'number') claimed.add(record.pid)
      }

      for (const [jobId, record] of Array.from(records.entries())) {
        if (record.killing) continue

        let snapshot
        try {
          snapshot = ctx.jobs.get(jobId, record.owner)
        } catch {
          records.delete(jobId)
          continue
        }
        if (snapshot === undefined || snapshot.status !== 'running') {
          records.delete(jobId)
          continue
        }

        // ── Señales de vida, en orden de fuerza ────────────────────────────
        let signal

        // 1. Escrituras en los archivos objetivo que el prompt nombró.
        for (const file of record.paths) {
          try {
            const info = await stat(file)
            if (info.mtimeMs > record.lastFileMtime) {
              record.lastFileMtime = info.mtimeMs
              if (info.mtimeMs > record.startedAt) signal = 'escritura en archivo objetivo'
            }
          } catch {
            // El hijo puede no haber creado el archivo todavía.
          }
        }

        // 2. El proceso del hijo: vivo, y con CPU o I/O nuevos desde el tick
        //    anterior. Esto no depende de que el hijo coopere.
        //    Se busca UNA sola vez: una vez visto el hijo, su desaparición es
        //    evidencia dura, y volver a buscar adoptaría el proceso de otro job.
        if (record.pid === undefined && !record.sawProcess && now >= record.nextDiscoveryAt) {
          record.nextDiscoveryAt = now + DISCOVERY_RETRY_MS
          const pid = await discoverChildPid(record, claimed)
          if (pid !== undefined) {
            record.pid = pid
            claimed.add(pid)
          }
        }
        const proc = record.pid === undefined ? undefined : await readProcStat(record.pid)
        if (proc === undefined) {
          if (record.pid !== undefined) {
            claimed.delete(record.pid)
            record.pid = undefined
          }
          // Evidencia dura: el proceso murió. Eso no es estancamiento, es muerte.
          if (record.sawProcess && now - record.startedAt > PROCESS_GRACE_MS) {
            killJob(jobId, record, 'watchdog: el proceso del hijo ya no existe (murió; evidencia dura, no estancamiento)')
            continue
          }
        } else {
          record.sawProcess = true
          const ioBytes = await readProcIo(record.pid)
          if (record.cpuTicks !== undefined && proc.cpuTicks > record.cpuTicks) signal = 'CPU del proceso'
          if (record.ioBytes !== undefined && ioBytes !== undefined && ioBytes > record.ioBytes) {
            signal = 'I/O del proceso'
          }
          record.cpuTicks = proc.cpuTicks
          record.ioBytes = ioBytes
        }

        // 3. Los hijos actuales son in-process: normalmente no tienen PID
        // propio que observar. Una herramienta abierta, dentro de su deadline,
        // es trabajo verificable y evita avisos falsos durante renders/tests.
        if (signal === undefined) {
          const flight = await childInFlight(record, now)
          if (flight !== undefined) {
            const minutes = Math.max(0, Math.round((now - flight.startedAt) / 60000))
            signal = `herramienta en vuelo (${flight.name}, ${minutes} min)`
          }
        }

        if (signal !== undefined) {
          record.lastSignalAt = now
          record.lastSignalKind = signal
        }

        const age = now - record.startedAt
        const silence = now - record.lastSignalAt
        const warnMs = record.preflight ? preflightWarnMs : warnSilenceMs
        const killMs = record.preflight ? preflightKillMs : killSilenceMs

        // Kill: sólo silencio TOTAL por encima del techo, o el runtime máximo.
        if (silence > killMs) {
          killJob(
            jobId,
            record,
            `watchdog: ${Math.round(silence / 60000)} min sin NINGUNA señal de vida — ${evidenceOf(record, now, proc)}`,
          )
          continue
        }
        if (age > maxRuntimeMs) {
          killJob(
            jobId,
            record,
            `watchdog: runtime máximo excedido (${Math.round(age / 60000)} min) — ${evidenceOf(record, now, proc)}`,
          )
          continue
        }
        // Aviso: una sola vez, y no mata nada.
        if (silence > warnMs && record.warnedAt === undefined) {
          record.warnedAt = now
          warnStall(jobId, record, now, proc, killMs)
        }
      }
    } finally {
      ticking = false
    }
  }

  ctx.effect(() => {
    const timer = globalThis.setInterval(() => {
      void tick().catch((error) => {
        ctx.logger?.warn?.(`orchestrator-job-watchdog: tick falló: ${String(error)}`)
      })
    }, pollIntervalMs)
    return () => globalThis.clearInterval(timer)
  }, 'orchestrator-job-watchdog: interval')
}
