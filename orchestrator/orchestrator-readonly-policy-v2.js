/**
 * Pins every session composed from the `Orquestador` preset to the READ-ONLY
 * sandbox mode and denies local production.
 *
 * The orchestrator must orchestrate and review, never produce. Its in-process
 * children share the workspace but receive a writable sandbox, so the
 * orchestrator can read, search, diff and run read-only checks while production
 * stays in the children.
 *
 * Two independent layers:
 *
 *  1. The MODE. `pin()` re-asserts `sandbox/mode: read-only` whenever the
 *     session's logged override is not read-only — not just once. The previous
 *     version pinned through a WeakSet, so a later switch to
 *     `danger-full-access` was permanent for that session; two audited sessions
 *     also never received the pin at all, because the preset was selected after
 *     the mode change. Re-asserting on every `agent/pre-step` closes both holes.
 *     The mode governs only THIS agent's tools; it does not touch the user's own
 *     edits in the UI. A producing agent needs a different preset.
 *
 *  2. The GUARD. `write`/`edit`/`str_replace_editor` are denied outright, and so
 *     is any `bash`/`pwsh` command that would produce files: output redirection
 *     to a real path, mutating shell verbs, in-place `sed`/`perl`, dependency
 *     installs, state-changing `git`, and heredocs whose body opens a file for
 *     writing. Read-only shell use — `ls`, `cat`, `grep`, `sed -n`, `git diff`,
 *     `git status`, non-writing test runners — stays available. This is defence
 *     in depth: the mode is the filesystem-level guarantee, and the guard
 *     catches the command-level cases the mode would otherwise allow under
 *     `danger-full-access`.
 *
 * The mode append is the same durable `sandbox/mode` switch the UI writes, so
 * it survives replay and every enforcing capability resolves it through the
 * shared sandbox policy.
 *
 * @module orchestrator-readonly-policy-v2
 */

export const name = 'orchestrator-readonly-policy'
export const inject = ['agents']

const PRESET_ID = 'orchestrator'
const MUTATION_TOOLS = new Set(['write', 'edit', 'str_replace_editor'])
const guarded = new WeakSet()
const MAX_JOB_WAIT_MS = 120_000
const MAX_CHILD_FOREGROUND_MS = 180_000

const PRODUCE_DENY =
  'El Orquestador no produce cambios localmente: delegá la modificación en un subagente y verificá el resultado.'
const SHELL_DENY =
  'El Orquestador usa el shell solo para leer y verificar, no para producir: delegá esta operación en un subagente.'
const WAIT_DENY =
  'No esperes un job más de 120 s en una sola llamada. Sondeá sin bloqueo o hacé trabajo útil entre esperas; tras tres sondeos sin evidencia, verificá el estado parcial antes de relanzar.'
const CHILD_FOREGROUND_DENY =
  'Un hijo no puede ejecutar un comando en primer plano por más de 180 s: usá un chequeo dirigido, dividí el trabajo o ejecutalo en background y recógelo con esperas de hasta 120 s.'
const SENSITIVE_DELEGATION_DENY =
  'El texto de delegación parece contener una credencial o pedir acceso a un archivo sensible. No se puede enviar a proveedores gratuitos: eliminá el secreto, reemplazalo por un placeholder o resolvelo localmente.'

const FREE_DELEGATION_TOOLS = new Set(['omniroute_code'])
const SENSITIVE_PROMPT_PATTERNS = [
  /-----BEGIN(?: [A-Z]+)? PRIVATE KEY-----/i,
  /(?:sk-|AIza|ghp_|github_pat_|xox[baprs]-)[A-Za-z0-9_=-]{16,}/,
  /\b(?:api[_-]?key|access[_-]?token|secret|password)\s*[:=]\s*["']?[A-Za-z0-9_./+=-]{12,}/i,
  /(?:^|[\s"'`])(?:\S*\/)?(?:\.env(?:\.[A-Za-z0-9_-]+)?|credentials(?:\.(?:ya?ml|json))?|id_rsa|\.npmrc)(?=$|[\s"'`])/i,
]

/**
 * Command-level mutation patterns for `bash`/`pwsh`. Conservative on purpose:
 * every pattern must be a real production verb, because a false positive blocks
 * legitimate read-only verification. Redirections must name a real path (so a
 * `>` inside a quoted expression does not trip it) and `/dev/null` stays allowed.
 */
const SHELL_MUTATION_PATTERNS = [
  // Output redirection to a real file, but not to a null/std stream and not to
  // the ephemeral scratch area: capturing verification output in /tmp is not
  // producing a project artifact.
  /(^|[^>])>{1,2}\s*(?:\/(?!dev\/(?:null|stdout|stderr)|tmp\/)|\.{1,2}[\w.-]*\/|~\/)[^\s;|&<>]+/,
  /(^|[^>])>{1,2}\s*[\w.-]+\.(?:html?|md|json|jsonl|txt|py|sh|bash|js|mjs|cjs|ts|tsx|jsx|css|scss|ya?ml|toml|ini|cfg|conf)\b/,
  // A heredoc whose body opens a file for writing.
  /<<[\s\S]*?(?:\.write_text\(|\.write_bytes\(|open\([^)]*['"][wa]b?['"])/,
  // Mutating shell verbs, at a command boundary.
  /(?:^|[;&|]\s*|\b(?:sudo|doas|timeout\s+\d+|xargs)\s+)(?:rm|rmdir|mv|cp|mkdir|touch|truncate|dd|chmod|chown|ln|tee|install|patch)\b/,
  /\bsed\s+(?:-i|--in-place)\b/,
  /\bperl\s+-i\b/,
  /\bgit\s+(?:add|commit|push|pull|reset|clean|checkout|restore|stash|merge|rebase|apply|rm|mv)\b/,
  /\b(?:npm|pnpm|yarn|bun)\s+(?:install|i|add|remove|uninstall|ci|link)\b/,
  /\b(?:pip|pip3|uv)\s+(?:install|add)\b/,
  /\b(?:apt|apt-get|dnf|yum|pacman|brew|snap)\s+(?:install|remove|upgrade|purge)\b/,
]

/**
 * @param {string} command - the raw shell command.
 * @returns {boolean} true when the command would produce or mutate files.
 */
function isProducingShellCommand(command) {
  return SHELL_MUTATION_PATTERNS.some((pattern) => pattern.test(command))
}

/**
 * Refuse waits that recreate the measured 5--10 minute idle loop. This guard
 * is intentionally shared by the root and its children: both can waste the
 * same wall-clock budget while waiting on a job.
 */
export function blindWaitReason(execution) {
  if (execution?.name !== 'job_output') return undefined
  const args = execution.arguments
  if (args === null || typeof args !== 'object' || args.wait !== true) return undefined
  const timeout = typeof args.timeout_ms === 'number' ? args.timeout_ms : 0
  return timeout > MAX_JOB_WAIT_MS ? WAIT_DENY : undefined
}

/**
 * A long, silent foreground command makes an in-process child indistinguishable
 * from a stalled child. Explicit long commands must instead be split or moved
 * to a background job with bounded polling.
 */
export function longForegroundReason(execution) {
  if (execution?.name !== 'bash' && execution?.name !== 'pwsh') return undefined
  const args = execution.arguments
  if (args === null || typeof args !== 'object' || args.run_in_background === true) return undefined
  const timeout = typeof args.timeoutMs === 'number' ? args.timeoutMs : 0
  return timeout > MAX_CHILD_FOREGROUND_MS ? CHILD_FOREGROUND_DENY : undefined
}

/**
 * Free providers may retain submitted text. Detect the common high-confidence
 * secret forms before a root delegates a prompt outside the local process.
 */
export function sensitiveDelegationReason(execution) {
  if (!FREE_DELEGATION_TOOLS.has(execution?.name)) return undefined
  const args = execution.arguments
  const prompt = args !== null && typeof args === 'object' && typeof args.prompt === 'string'
    ? args.prompt
    : ''
  return SENSITIVE_PROMPT_PATTERNS.some((pattern) => pattern.test(prompt))
    ? SENSITIVE_DELEGATION_DENY
    : undefined
}

/**
 * True for an agent that is a DELEGATED CHILD rather than the orchestrator.
 *
 * The preset forbids the orchestrator from producing; it does not forbid its
 * children. With the in-process delegation path (`omniroute_code`) a child
 * shares the session and the workspace, so pinning it read-only would make the
 * whole no-ACP design unable to write a single file. The session header is
 * authoritative: a child carries `parentSession` and a non-zero
 * `delegationDepth`.
 *
 * @param agent - the agent to classify.
 * @returns whether the agent is a delegated child.
 */
function isDelegatedChild(agent) {
  const header = agent?.session?.header
  if (header === undefined || header === null) return false
  if (header.parentSession !== undefined) return true
  return typeof header.delegationDepth === 'number' && header.delegationDepth > 0
}

/**
 * Verbos que piden la contraseña del usuario en el escritorio (polkit) o
 * escalan privilegios. Un hijo del Orquestador nunca debe usarlos: el 2026-09-24
 * un hijo probó `systemd-run` (bus de SISTEMA, sin `--user`) una decena de veces
 * para dejar el Studio vivo, y cada intento abrió un diálogo de contraseña en
 * GNOME; algunos terminaron en units `root` tiradas en el sistema.
 */
const FORBIDDEN_CHILD_VERBS = new Set(['sudo', 'doas', 'pkexec', 'su'])
/** Palabras que preceden al verbo real sin cambiarlo. */
const VERB_PREFIXES = new Set(['nohup', 'setsid', 'env', 'command', 'exec'])
const CHILD_ESCALATION_DENY =
  'Un hijo del Orquestador no administra units ni escala privilegios: `sudo`, `pkexec` y el bus de SISTEMA de systemd (sin `--user`) abren un diálogo de contraseña en el escritorio del usuario. El Studio YA corre como unit de usuario: usá `systemctl --user restart hf-studio-g2` (o `hf-studio-descente`), o `bash scripts/serve.sh` para una prueba puntual.'

/**
 * ¿Este comando pide contraseña o escala privilegios?
 *
 * Se analiza por segmentos (`;`, `|`, `&&`, saltos de línea) y sólo el PRIMER
 * verbo de cada uno: así `grep systemctl README.md` no da falso positivo.
 *
 * @param command - la línea de shell.
 * @returns true cuando hay que negarla.
 */
function isChildEscalation(command) {
  for (const raw of String(command || '').split(/\n|;|\|\||&&|\|/)) {
    let words = raw.trim().split(/\s+/).filter((word) => word !== '')
    while (words.length > 0) {
      const head = words[0]
      if (head === 'timeout') words = words.slice(2)
      else if (VERB_PREFIXES.has(head) || /^[A-Za-z_][A-Za-z0-9_]*=/.test(head)) words = words.slice(1)
      else break
    }
    const verb = words[0]
    if (verb === undefined) continue
    if (FORBIDDEN_CHILD_VERBS.has(verb)) return true
    if ((verb === 'systemctl' || verb === 'systemd-run') && !words.includes('--user')) return true
  }
  return false
}

/** Agentes hijos que ya tienen su guard instalado. */
const childGuarded = new WeakSet()

/**
 * Guard del HIJO: sólo niega lo que dispara un diálogo de contraseña. No toca
 * escrituras ni comandos normales (el hijo SÍ produce).
 *
 * @param ctx - the preset scope context.
 * @param agent - the delegated child agent.
 */
function guardChildEscalations(ctx, agent) {
  if (childGuarded.has(agent)) return
  childGuarded.add(agent)
  try {
    agent.ctx.inject(['tools'], (scope) => {
      scope.tools.guard((execution) => {
        const wait = blindWaitReason(execution)
        if (wait !== undefined) return wait
        if (execution.name !== 'bash' && execution.name !== 'pwsh') return undefined
        const foreground = longForegroundReason(execution)
        if (foreground !== undefined) return foreground
        const args = execution.arguments
        const command =
          args !== null && typeof args === 'object' && typeof args.command === 'string'
            ? args.command
            : ''
        return command !== '' && isChildEscalation(command) ? CHILD_ESCALATION_DENY : undefined
      })
    })
  } catch (error) {
    ctx.logger?.warn?.(
      `orchestrator-readonly-policy: no pude instalar el guard de escalada en ${String(agent.id)}: ${error instanceof Error ? error.message : String(error)}`,
    )
  }
}

/**
 * Sandbox mode the USER chose per root session, captured just before the preset
 * pins that session read-only. Children get it back: the delegation machinery
 * copies the parent's EFFECTIVE mode onto the child
 * (`captureDelegatedPolicyOverrides` in dsh-subagent), so without this a child
 * of a pinned orchestrator would inherit read-only and could not write a single
 * file — which would defeat the whole in-process delegation path.
 */
const userModes = new Map()
/** Fallback for a child whose parent's user mode was never observed. */
const FALLBACK_CHILD_MODE = 'workspace-write'

/**
 * Read one agent's effective sandbox mode, defensively.
 *
 * @param ctx - the preset scope context.
 * @param agent - the agent whose session is read.
 * @returns the logged mode, or `undefined` when it cannot be read.
 */
function currentMode(ctx, agent) {
  try {
    return ctx.sandboxPolicy?.overrideOf?.(agent.session)
  } catch (error) {
    ctx.logger?.warn?.(
      `orchestrator-readonly-policy: no pude leer el modo de ${String(agent.id)}: ${error instanceof Error ? error.message : String(error)}`,
    )
    return undefined
  }
}

/**
 * Give a delegated child the mode its parent's USER chose, overriding the
 * read-only the delegation copied from the pinned parent. Later child switches
 * win over the delegation events, so appending here is enough.
 *
 * @param ctx - the preset scope context.
 * @param agent - the delegated child agent.
 */
function restoreChildMode(ctx, agent) {
  const parentSession = agent.session?.header?.parentSession
  const remembered = parentSession === undefined ? undefined : userModes.get(parentSession)
  const mode = remembered ?? FALLBACK_CHILD_MODE
  if (currentMode(ctx, agent) === mode) return
  try {
    agent.session.append('sandbox/mode', { mode })
    ctx.logger?.info?.(
      `orchestrator-readonly-policy: hijo ${String(agent.id)} en modo "${mode}" (puede escribir; el read-only es del orquestador).`,
    )
  } catch (error) {
    ctx.logger?.warn?.(
      `orchestrator-readonly-policy: no pude devolver el modo "${mode}" al hijo ${String(agent.id)}: ${error instanceof Error ? error.message : String(error)}`,
    )
  }
}

/**
 * Re-assert read-only and install the production guard.
 *
 * @param ctx - the preset scope context, with `agents` injected.
 * @param agent - the agent to pin.
 */
function pin(ctx, agent) {
  if (agent === undefined || agent === null) return
  if (isDelegatedChild(agent)) {
    restoreChildMode(ctx, agent)
    guardChildEscalations(ctx, agent)
    return
  }

  // Read the logged override defensively: `sandboxPolicy` is a base service, but
  // it is deliberately NOT in `inject`, so that a deployment without it still
  // loads this plugin and falls back to the original always-append behaviour.
  const current = currentMode(ctx, agent)
  if (current !== undefined && current !== 'read-only') {
    // Remember what the USER chose before we overwrite it: every child gets it
    // back (see restoreChildMode).
    userModes.set(agent.session.id, current)
  }

  if (current !== 'read-only') {
    try {
      agent.session.append('sandbox/mode', { mode: 'read-only' })
      if (current !== undefined) {
        ctx.logger?.warn?.(
          `orchestrator-readonly-policy: la sesión ${String(agent.id)} tenía el modo "${current}"; lo devolví a read-only (el preset Orquestador no produce).`,
        )
      }
    } catch (error) {
      ctx.logger?.warn?.(
        `orchestrator-readonly-policy: no pude fijar la sesión ${String(agent.id)} en read-only: ${error instanceof Error ? error.message : String(error)}`,
      )
    }
  }

  if (guarded.has(agent)) return
  guarded.add(agent)

  try {
    agent.ctx.inject(['tools'], (scope) => {
      scope.tools.guard((execution) => {
        const sensitive = sensitiveDelegationReason(execution)
        if (sensitive !== undefined) return sensitive
        const wait = blindWaitReason(execution)
        if (wait !== undefined) return wait
        if (MUTATION_TOOLS.has(execution.name)) return PRODUCE_DENY

        if (execution.name === 'bash' || execution.name === 'pwsh') {
          const args = execution.arguments
          const command =
            args !== null && typeof args === 'object' && typeof args.command === 'string'
              ? args.command
              : ''
          if (command !== '' && isProducingShellCommand(command)) return SHELL_DENY
        }

        return undefined
      })
    })
  } catch (error) {
    ctx.logger?.warn?.(
      `orchestrator-readonly-policy: no pude instalar el guard de mutaciones en ${String(agent.id)}: ${error instanceof Error ? error.message : String(error)}`,
    )
  }
}

/**
 * Register the lifecycle listeners that pin each joined agent.
 *
 * @param ctx - the preset scope context, with `agents` injected.
 */
export function apply(ctx) {
  ctx.on('agent/created', ({ agent }) => pin(ctx, agent))
  ctx.on('agent/session-start', ({ agent }) => pin(ctx, agent))
  ctx.on('agent/pre-step', ({ agent }, next) => {
    pin(ctx, agent)
    return next()
  })
  ctx.on('agent-preset/selected', (sessionId, agentPreset) => {
    if (agentPreset !== PRESET_ID) return
    pin(ctx, ctx.agents.get(sessionId))
  })
}
