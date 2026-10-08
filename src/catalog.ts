/**
 * Live model catalog for the ChatGPT-plan route.
 *
 * The route advertises its real catalog at `GET /v1/models`, including per
 * model display names, descriptions, context windows, input modalities, and
 * the exact reasoning levels each model accepts. Hardcoding that list drifts:
 * it hides models the account can actually use and offers reasoning levels the
 * route rejects.
 *
 * @module dsh-llm-siwc/catalog
 */

/** One model as the live endpoint describes it. */
export interface CatalogModel {
  /** Wire id used by `GenerateOptions.model`. */
  slug: string
  /** Human-facing label. */
  displayName: string
  description?: string
  contextWindow: number
  maxContextWindow?: number
  inputModalities: readonly string[]
  /** Accepted reasoning levels, in the order the route lists them. */
  reasoningLevels: readonly string[]
  defaultReasoningLevel?: string
}

/**
 * The catalog to fall back to when the live endpoint cannot be read.
 *
 * Deliberately a subset of what the route serves: advertising a model that the
 * account cannot use is worse than omitting one, and reasoning levels are the
 * conservative intersection across models.
 */
export const FALLBACK_CATALOG: readonly CatalogModel[] = [
  'gpt-6.1-sol',
  'gpt-6-astra',
  'gpt-6-sol',
  'gpt-6-luna',
  'gpt-5.6-sol',
  'gpt-5.6-terra',
  'gpt-5.6-luna',
].map((slug) => ({
  slug,
  displayName: slug,
  contextWindow: 272_000,
  inputModalities: ['text', 'image'],
  reasoningLevels: ['low', 'medium', 'high'],
}))

/** Human-facing labels and hints for the levels the route accepts. */
const REASONING_LABELS: Record<string, { name: string; description: string }> = {
  minimal: { name: 'Minimal', description: 'Barely any reasoning; fastest.' },
  low: { name: 'Low', description: 'Light reasoning for routine, latency-sensitive work.' },
  medium: { name: 'Medium', description: 'Balanced reasoning.' },
  high: { name: 'High', description: 'The default balance for most tasks.' },
  xhigh: { name: 'Extra high', description: 'More deliberation for hard problems.' },
  max: { name: 'Max', description: 'Maximum reasoning effort.' },
  ultra: { name: 'Ultra', description: 'The route\'s highest reasoning level.' },
}

/** Effort order used when a model's own order is unavailable. */
const EFFORT_ORDER = ['minimal', 'low', 'medium', 'high', 'xhigh', 'max', 'ultra']

/** Turn one accepted level into the picker's shape. */
export function reasoningEffort(level: string): { id: string; name: string; description?: string } {
  const label = REASONING_LABELS[level]
  return label === undefined
    ? { id: level, name: level }
    : { id: level, name: label.name, description: label.description }
}

/** Sort levels into the route's natural order, unknown levels last. */
export function orderLevels(levels: readonly string[]): string[] {
  return [...levels].sort((left, right) => {
    const a = EFFORT_ORDER.indexOf(left)
    const b = EFFORT_ORDER.indexOf(right)
    return (a === -1 ? EFFORT_ORDER.length : a) - (b === -1 ? EFFORT_ORDER.length : b)
  })
}

interface RawModel {
  slug?: unknown
  display_name?: unknown
  description?: unknown
  context_window?: unknown
  max_context_window?: unknown
  input_modalities?: unknown
  default_reasoning_level?: unknown
  supported_reasoning_levels?: unknown
}

/** Read one raw entry defensively; the endpoint is a preview surface. */
function toCatalogModel(raw: RawModel): CatalogModel | undefined {
  const slug = typeof raw.slug === 'string' && raw.slug.length > 0 ? raw.slug : undefined
  if (slug === undefined) return undefined
  const levels = Array.isArray(raw.supported_reasoning_levels)
    ? raw.supported_reasoning_levels.flatMap((entry) => {
        const effort = (entry as { effort?: unknown } | null)?.effort
        return typeof effort === 'string' && effort.length > 0 ? [effort] : []
      })
    : []
  const modalities = Array.isArray(raw.input_modalities)
    ? raw.input_modalities.filter((value): value is string => typeof value === 'string')
    : []
  return {
    slug,
    displayName:
      typeof raw.display_name === 'string' && raw.display_name.length > 0 ? raw.display_name : slug,
    ...(typeof raw.description === 'string' && raw.description.length > 0
      ? { description: raw.description }
      : {}),
    contextWindow: typeof raw.context_window === 'number' ? raw.context_window : 272_000,
    ...(typeof raw.max_context_window === 'number'
      ? { maxContextWindow: raw.max_context_window }
      : {}),
    inputModalities: modalities.length > 0 ? modalities : ['text'],
    reasoningLevels: orderLevels(levels),
    ...(typeof raw.default_reasoning_level === 'string'
      ? { defaultReasoningLevel: raw.default_reasoning_level }
      : {}),
  }
}

export interface CatalogOptions {
  /** Resolve a currently valid access token. */
  resolveAccessToken: () => Promise<string>
  /** API base URL; defaults to the public endpoint. */
  baseUrl?: string
  fetchImpl?: typeof fetch
  /** How long a successful read is reused. */
  ttlMs?: number
  /** Models to advertise while the first read is in flight or after a failure. */
  seed?: readonly CatalogModel[]
  /** Diagnostics. */
  onError?: (error: unknown) => void
}

const MODELS_PATH = '/v1/models'
const DEFAULT_TTL_MS = 5 * 60_000

/**
 * Reads and caches the route's model catalog.
 *
 * Concurrent callers share one in-flight request, and a failed refresh keeps
 * serving the previous result rather than emptying the picker.
 */
export class ModelCatalog {
  readonly #options: CatalogOptions
  #cached: readonly CatalogModel[] | undefined
  #cachedAt = 0
  #inFlight: Promise<readonly CatalogModel[]> | undefined

  constructor(options: CatalogOptions) {
    this.#options = options
  }

  /** Last known catalog without any I/O, or the seed when nothing is known. */
  snapshot(): readonly CatalogModel[] {
    return this.#cached ?? this.#options.seed ?? FALLBACK_CATALOG
  }

  /** One model from the last known catalog. */
  find(slug: string): CatalogModel | undefined {
    return this.snapshot().find((model) => model.slug === slug)
  }

  /** Refresh if stale, then return the catalog. Never throws. */
  async load(signal?: AbortSignal): Promise<readonly CatalogModel[]> {
    if (this.#cached !== undefined && Date.now() - this.#cachedAt < (this.#options.ttlMs ?? DEFAULT_TTL_MS)) {
      return this.#cached
    }
    this.#inFlight ??= this.#fetch(signal).finally(() => {
      this.#inFlight = undefined
    })
    return this.#inFlight
  }

  async #fetch(signal?: AbortSignal): Promise<readonly CatalogModel[]> {
    try {
      const token = await this.#options.resolveAccessToken()
      const fetchImpl = this.#options.fetchImpl ?? fetch
      const response = await fetchImpl(`${this.#options.baseUrl ?? 'https://api.openai.com'}${MODELS_PATH}`, {
        headers: { Authorization: `Bearer ${token}` },
        signal,
      })
      if (!response.ok) throw new Error(`models request failed with HTTP ${response.status}`)
      const body = (await response.json()) as { models?: unknown }
      const models = Array.isArray(body.models)
        ? body.models.flatMap((entry) => {
            const model = toCatalogModel((entry ?? {}) as RawModel)
            return model === undefined ? [] : [model]
          })
        : []
      if (models.length === 0) throw new Error('models response carried no usable entries')
      this.#cached = models
      this.#cachedAt = Date.now()
      return models
    } catch (error) {
      this.#options.onError?.(error)
      // Keep any previous result; otherwise the seed keeps the picker usable.
      return this.snapshot()
    }
  }
}
