/**
 * 「模型接力」这条 provider 路由的适配器。
 *
 * 契约（宿主 @deepseek-ai/dsh-llm 的 LlmAdapter）：
 *   providerInfo(provider) -> { id, name }
 *   providerRetryPolicy(provider) -> ResolvedRetryPolicy | undefined
 *   imageRequestPricing(provider, model) -> undefined
 *   listModels(provider) -> Promise<LlmModelInfo[]>
 *   resolveModel(provider, model, signal) -> Promise<LlmResolvedModelInfo>
 *   prepareCall(provider, model, signal) -> Promise<{ model, stream(options) }>
 *   stream(options) -> AsyncIterable<StreamChunk>
 *
 * 本插件是「包装路由」（wrapper route）：在自己的 stream() 体内再调用一次
 * ctx.llm.stream()，把 provider/model 换成队列里的真实模型。modlens、
 * vision-router 用的都是这个模式，dsh-cost-meter 的注释里把它写得很明确。
 *
 * 整个设计的关键只有一条 —— **提交点**：
 * 候选模型产出的 chunk 先全部扣在手里，直到出现「用户会看见的答案内容」
 * （非空白 text-delta、tool-call-delta，或非 reasoning 的 block-end）才整批
 * 放行。在此之前失败，这一整轮就被整根丢掉，调用方一个字节都没看见，于是
 * 换模型是无声的、不会把两个模型的文字拼在一起。
 *
 * reasoning 的 delta 故意不算提交点：「思考了一分钟然后撞上额度墙」正是最
 * 值得切换的情形，扣住的思考跟着这一轮一起丢，调用方根本不知道发生过。
 */

import {
  CODE_IDLE_TIMEOUT,
  CODE_MANUAL_SKIP,
  CODE_NO_VISIBLE_OUTPUT,
  DEFAULT_TEMPLATE_ID,
  MAX_COOLDOWN_SEC,
  MAX_SAME_MODEL_RETRIES,
  MAX_TEMPLATE_CANDIDATES,
  MODEL_DESCRIPTION,
  NON_FAILOVER_CODES,
  ROUTE,
  ROUTE_LABEL,
  routeKey,
  splitKey,
} from './constants.js'
import { isTransientFailure, cooldownMsFor, Cooldowns } from './cooldown.js'
import { Concurrency, concurrencyOf } from './limits.js'
import { isLocalNetworkFailure } from './network.js'

/** 自动补发的「继续」最多堆几条，防止候选很多时把会话撑长。 */
const MAX_CONTINUES = 3

/**
 * 同一个模型最多试几次（含第一次）。
 *
 * 旧配置里只有一个布尔开关 `retrySameModelOnce`，它表达的是「多试一次」；
 * 现在这件事要能调，因为同一个模型背后可能是好几个账号。没设过新键时按
 * 老开关折算：关掉过就是只试一次，否则用默认值。
 */
function sameModelTries(settings) {
  const raw = settings?.sameModelRetries
  if (Number.isInteger(raw) && raw >= 0) return Math.min(raw, MAX_SAME_MODEL_RETRIES) + 1
  return (settings?.retrySameModelOnce === false ? 0 : 2) + 1
}

/**
 * 同一个模型连着撞几次输出上限之后，就不让它接着写，换队列里的下一个。
 *
 * 撞上限本身不算「这个模型坏了」—— 它只是还没写完，让它接着说最省事，所以
 * **第一次**照旧用原模型补「继续」。但如果它连着撞，那就是「这个模型在这个
 * 任务上写不完」：要么上下文已经撑满，要么它习惯性收不住。这时候继续在它身上
 * 耗着，用户看到的只是同一句半截话被反复截断，不如换个人接手 —— 队列里的下
 * 一个模型读到的是同一段对话（含半截回答），接着往下写。
 *
 * 取 2 的语义：第 1 次撞上限用原模型补一次；第 2 次还撞，下一次调用就轮到
 * 队列里的下一个。写成常量而不是设置项，是不想让设置页再多一行 —— 想调随时
 * 改这里。
 */
const CEILING_STREAK_BEFORE_ROTATE = 2

/**
 * 宿主默认的 backoff 值，抄自 dsh-llm 的 retry-policy 默认。
 *
 * 注意形状：这只是**过桥的原料**。适配器必须交出 resolveRetryPolicy() 的
 * 返回值形状 —— 也就是把 initialDelayMs / maxDelayMs / jitterRatio 摊平在顶层
 * （见 dsh-llm/lib/types/retry-policy.js:110-115 和 84-91）。配置文件里的
 * `backoff: {...}` 嵌套形状是**另一种**形状，只属于 RetryPolicySchema 输入；
 * 把嵌套形状交给宿主，llm-retry 读到的 delay 全是 undefined，退避时间直接变
 * 成 NaN 并把持久化事件写坏。
 */
const DEFAULT_BACKOFF = { initialDelayMs: 500, maxDelayMs: 10_000, jitterRatio: 0.1 }

/**
 * 一个永远不会匹配任何真实错误码的哨兵。
 *
 * resolveRetryPolicy 有一条「retryableCodes 不能为空」的校验，而适配器的返回值
 * **不经过**这道校验（dsh-llm/lib/index.js:1867 是 `??` 直接采用）。用哨兵而不是
 * 空数组，是为了万一将来有一层替我们再校验一次也不至于把插件整挂。
 */
const NEVER_RETRY = '__model-failover-never__'

/**
 * 这个 chunk 是否代表「调用方已经看见了答案」。
 *
 * 判据与宿主 assistant-stream 的可见性定义对齐：只有非空白的文本才是答案，
 * 流式工具调用一旦开始就意味着这轮已经有产品，reasoning 与它的 block-end
 * 都不算 —— 让 reasoning 的 block-end 提交，会把队列钉死在它本该熬过去的
 * 那次失败之前一个 chunk。
 */
export function commitsAnswer(chunk) {
  if (chunk === null || typeof chunk !== 'object') return false
  if (chunk.type === 'text-delta') return typeof chunk.text === 'string' && chunk.text.trim() !== ''
  if (chunk.type === 'tool-call-delta') return true
  if (chunk.type !== 'block-end') return false
  if (chunk.block?.type === 'reasoning') return false
  if (chunk.block?.type === 'text') return typeof chunk.block.text === 'string' && chunk.block.text.trim() !== ''
  return true
}

/**
 * 上游这句话是不是在说「你这一坨太大了」。
 *
 * 为什么要专门认它：撞上下文窗口时，**接力是最坏的应对**。换下一个模型，
 * 发过去的是同一坨一模一样的巨型请求，于是队列里每个模型排着队各撞一次墙、
 * 各被隔离一天，而历史一个字节都没少 —— 用户看到的就是「越失败越大，最后
 * 整个会话都崩了」。
 *
 * 该做的是把这种失败原样交回给 agent loop，让它先把历史压下去（宿主认得
 * `CONTEXT_WINDOW_EXCEEDED` 这个码，走的就是压缩那条路）。所以判据只看**报错
 * 文本**：上游把这件事说得五花八门，HTTP 码却常常只是 400 / 413 / 422。
 *
 * @param {string} message
 * @returns {boolean}
 */
export function looksLikeContextOverflow(message) {
  if (typeof message !== 'string' || message === '') return false
  const text = message.toLowerCase()
  if (/context[_ ]length[_-]exceeded|maximum context length|context window exceeded/.test(text)) return true
  if (/exceeds? the (?:model )?context window|context window (?:is )?(?:too small|exceeded|overflow)/.test(text)) return true
  if (/prompt is too long|input is too long|request (?:is )?too large|payload too large|body too large/.test(text)) return true
  if (/too many tokens|reduce the length of the messages/.test(text)) return true
  // 本机那条拦截层（billion-context 的后备拒绝）原话就长这样。
  if (/bili_gate_oversize_refusal|oversize_refusal/.test(text)) return true
  if (/\boversize\b.*\b(?:context|token|body|request)\b/.test(text)) return true
  return false
}

/** 把一次 finish 或一个异常收敛成 `{ code, message, status? }`。 */
export function rawFailureOf(finish, error) {
  if (error !== undefined && error !== null) {
    return {
      code: typeof error.code === 'string' && error.code !== '' ? error.code : 'UNKNOWN',
      message: String(error.message ?? error),
      status: Number.isFinite(error.status) ? error.status : undefined,
    }
  }
  const reason = finish?.reason
  if (reason?.kind === 'error' || reason?.kind === 'aborted') {
    return {
      code: typeof reason.failure?.code === 'string' && reason.failure.code !== '' ? reason.failure.code : reason.kind.toUpperCase(),
      message: String(reason.failure?.message ?? ''),
      status: Number.isFinite(reason.failure?.status) ? reason.failure.status : undefined,
    }
  }
  if (finish?.type === 'finish') {
    return {
      code: CODE_NO_VISIBLE_OUTPUT,
      message: `模型以 ${String(reason?.kind ?? 'unknown')} 结束，但整轮没有产出任何可见内容。`,
    }
  }
  return { code: CODE_NO_VISIBLE_OUTPUT, message: '模型没有产出任何内容。' }
}

/**
 * 收敛之后再校准一次错误码。
 *
 * 见 `looksLikeContextOverflow()`：上游用 HTTP 400 / 413 / 422 或一句自定义
 * 文案来表达「请求太大」，这些码本身没有语义，宿主认不出来就不会去压缩，于是
 * 接力帮它把这一坨巨型请求发遍整个队列。这里统一改判成 `CONTEXT_WINDOW_EXCEEDED`，
 * 它在 `NON_FAILOVER_CODES` 里 —— 不隔离、不换人，原样交回给 agent loop。
 *
 * @param {any} finish
 * @param {any} error
 * @returns {{code: string, message: string, status?: number}}
 */
export function failureOf(finish, error) {
  const failure = rawFailureOf(finish, error)
  if (failure.code !== 'CONTEXT_WINDOW_EXCEEDED' && looksLikeContextOverflow(failure.message)) {
    return { ...failure, code: 'CONTEXT_WINDOW_EXCEEDED' }
  }
  return failure
}

/** 一条 `RequestUserInput`；宿主 Message 联合类型里唯一允许不带 id/source 的形态。 */
export function continueMessage(text) {
  return { role: 'user', content: [{ type: 'text', text }] }
}

/**
 * 默认的等待实现。可以用 `--use-system-ca` 之外的方式覆盖（测试会注入假实现）。
 *
 * 带 abort 支持：用户在等待期间按了停止，这一轮该立刻收摊，而不是等满再往下试。
 *
 * @param {number} ms - 等待毫秒数。
 * @param {AbortSignal | undefined} signal - 调用方给的取消信号。
 * @returns {Promise<void>}
 */
function sleep(ms, signal) {
  if (signal?.aborted === true) return Promise.resolve()
  return new Promise((resolve) => {
    const done = () => {
      clearTimeout(timer)
      signal?.removeEventListener?.('abort', done)
      resolve()
    }
    const timer = setTimeout(done, ms)
    signal?.addEventListener?.('abort', done, { once: true })
  })
}

/**
 * @param {object} deps
 * @param {any} deps.llm - 宿主 llm 服务（借它做嵌套调用）。
 * @param {import('./store.js').JsonFile<any>} deps.settings
 * @param {import('./health.js').DailyHealth} deps.health
 * @param {import('./catalog.js').Catalog} deps.catalog
 * @param {{ push: (event: object) => void, warn: (message: string) => void }} deps.log
 * @param {import('./cooldown.js').Cooldowns} [deps.cooldowns] - 限流冷却表；缺席时自己建一份。
 * @param {() => number} [deps.now]
 * @param {(ms: number, signal?: AbortSignal) => Promise<void>} [deps.sleep] - 等待实现；测试用。
 */
export class FailoverAdapter {
  constructor({ llm, settings, health, catalog, log, notes, cooldowns, limits, now = Date.now, sleep: wait = sleep }) {
    this.llm = llm
    this.settings = settings
    this.health = health
    this.catalog = catalog
    this.log = log
    /**
     * 「这个会话刚才跑的是谁」的便签（lib/notes.js）。可以缺席 —— 缺席时只是
     * 不显示小字，接力本身一个字节都不受影响。
     */
    this.notes = notes
    this.now = now
    this.sleep = wait
    /**
     * 限流冷却表（lib/cooldown.js）。
     *
     * 和 `health`（当日隔离）是**两件不同的事**：隔离按「会话 × 模板」记一整天，
     * 冷却不分会话、只活几十秒。限流是「等一下就好」，把它记成「今天不行」正是
     * 用户报的那个问题。缺席时自己建一份，适配器可以单独拿来用（测试就是这么做的）。
     */
    this.cooldowns = cooldowns ?? new Cooldowns({ now })
    /**
     * 每个候选自己的**并发上限**占用表（lib/limits.js）。
     *
     * 隔离与冷却是「这个候选自己不行」，这里是「这个候选同时只收得下 N 个会话」——
     * 第三种「先别用它」。区别在于**谁**在用：占满的时候它一点毛病都没有，只是
     * 别人正占着，所以这里不隔离、不冷却，只把这一趟的选人绕开它。
     *
     * 缺席时自己建一份：适配器可以单独拿来用（测试就是这么做的），而且没配任何
     * 上限时这本账完全是空转 —— `usable()` 对没设上限的候选一律说能进。
     */
    this.limits = limits ?? new Concurrency({ now })
    /**
     * 会话 → `{ hits, key, provider, model }`：这个会话**连着**撞了几次输出上限。
     *
     * `hits` 每撞一次加一，成功写完一轮就整条删掉。它是「换人」的唯一依据：
     * 每攒够 CEILING_STREAK_BEFORE_ROTATE 次，队列就往前挪一格。账按会话累加
     * 而不是按模型，否则换过人之后下一轮又会回到队首，在两个模型之间来回打转。
     *
     * 只在内存里。撞上限这件事只有适配器自己看得见 —— 轮级那层只拿到一个
     * `{ kind: 'max-tokens' }`，不知道是谁跑的。
     */
    this.ceiling = new Map()
    /**
     * 会话 → `{ provider, model, key, startedAt }`：此刻**正在**跑的那个候选。
     *
     * 会话页那一栏靠它做两件事：把正在跑的会话列出来（哪怕它还没跑完任何一趟、
     * 因此既没隔离也没验证记录），以及在有人跑着的时候把轮询间隔从 15 秒压到
     * 2 秒 —— 用户盯着看的就是这一刻，15 秒的刷新等于没在更新。
     *
     * 同样是内存态：进程重启之后没人「正在跑」，本来也不该留着。
     */
    this.live = new Map()
    /**
     * 被用户在会话页点了「跳过」的会话：`sessionId → key`。
     *
     * 只存「下一次调用要从谁之后开始」这件事，不用来隔离 —— 隔离是
     * `health.markBroken` 写进盘里的账，走的是同一条路，和「这个模型自己报错」
     * 完全一样。这里多一份只是为了让**正在跑的那一趟**能被当场叫停：用户按下去
     * 的时候，请求往往正挂着等上游，光写隔离名单它还会接着跑完，用户看到的是
     * 「按了没反应」。真正的打断靠 `#runStream` 每一跳开工前查一次这里。
     *
     * 值里带 `at`：`skip` 与 `stream` 之间隔着一次 HTTP 往返，比这更早的请求
     * 不该被这一次的意图带偏（它可能已经在跨会话并发的另一趟里了）。
     */
    this.skipped = new Map()
    /**
     * 正在跑的那一趟给它留下的「叫醒我」钩子：`sessionId → Set<(key) => void>`。
     *
     * 光靠 `#skipPending` 在每次取 chunk 之前查一遍是不够的 —— 用户点跳过的时候，
     * 上游最常见的姿态恰恰是**一个 chunk 都不来**（挂住、连接半开、排队里没轮到）。
     * 那种情况下循环正卡在 `await nextChunk()` 里，永远走不到下一次检查，用户看到的
     * 还是「按了没反应」。所以这里再挂一组监听，`noteSkip` 一响就把那次 await 叫醒。
     *
     * 监听在 `#attempt` 收尾时一定要摘掉：它按会话号存，会话不结束就不会被回收，
     * 攒下去就是一条隐形的泄漏。
     */
    this.skipWatchers = new Map()
  }

  /**
   * 这个会话此刻正在跑的候选，没有就 undefined。
   *
   * @param {string} [sessionId]
   */
  liveFor(sessionId) {
    if (typeof sessionId !== 'string' || sessionId === '') return undefined
    return this.live.get(sessionId)
  }

  /**
   * 用户在会话页按了「跳过」：把这个候选写进今天的隔离名单，并且**当场**掐掉
   * 正在跑的那一趟，让队列接着换下一个。
   *
   * 为什么不是前端自己调 `/reset` 加改设置：隔离名单是「会话 × 模板 × 候选」
   * 三元的，前端手里只有一堆显示用字段；真正的写入口在 `health`，只有宿主拿得到。
   *
   * 为什么还要 `noteSkip`：见那个方法上的注释 —— 写名单只影响**下一次**调用，
   * 而用户按下按钮时请求往往正挂着。
   *
   * 名额立刻交还（`cool: false`）：用户说的是「别用它了」，挂一笔 5 秒收工冷却
   * 只会让下一个候选上得更慢；何况它今天已经不再接客。
   *
   * @param {{sessionId?: string, templateId?: string, key?: string}} target
   * @returns {{ok: boolean, key?: string, provider?: string, model?: string, live?: boolean}}
   */
  skip({ sessionId, templateId, key } = {}) {
    const live = this.liveFor(sessionId)
    // 没给 key 就是「跳过它此刻正在跑的那个」—— 会话页那颗按钮不用自己记状态。
    const target = typeof key === 'string' && key !== '' ? key : (live?.key ?? '')
    if (target === '') return { ok: false }
    const scope = typeof templateId === 'string' && templateId !== ''
      ? templateId
      : (typeof live?.templateId === 'string' && live.templateId !== '' ? live.templateId : DEFAULT_TEMPLATE_ID)
    const { provider, model } = splitKey(target)
    // 正在跑的那一趟先拿到「该让位了」的便条，再写账：反过来的话，写账那一刻
    // 循环可能正好走到下一跳的判断上，白白多跑一次这个候选。
    const running = live !== undefined && live.key === target
    if (running) this.noteSkip(sessionId, target)
    this.health.markBroken(target, {
      code: CODE_MANUAL_SKIP,
      message: '用户点了跳过',
      at: this.now(),
      sessionId,
      templateId: scope,
    })
    this.health.file.flush()
    // 隔离了就别再让它挂在限流冷却里：两笔账叠着，界面会同时说「要等 40 秒」
    // 和「今天别用它」，只有后者是真的。
    this.cooldowns.delete(target)
    this.limits.free(target, sessionId, { cool: false })
    this.log.push({
      at: this.now(),
      kind: 'skip',
      key: target,
      provider,
      model,
      sessionId,
      templateId: scope,
      // 前端据此分两句话说：按下去时它正在跑，还是只是排在队首。
      live: running,
    })
    this.log.warn(`model-failover: 用户跳过了 ${provider}/${model}，今天不再用它`)
    return { ok: true, key: target, provider, model, live: running }
  }

  /** 所有正在跑的会话，`Map<sessionId, {provider, model, key, startedAt}>`。 */
  liveSessions() {
    return this.live
  }

  /**
   * 记下「用户在这一趟里点了跳过」，让正在跑的那一次调用当场让位。
   *
   * 为什么要单独记一笔，而不是只写隔离名单：`/skip` 是个 HTTP 请求，它写进
   * `health` 的隔离要等到**下一次**调用才会生效 —— 而用户按下按钮时，请求
   * 往往正挂在一次已经发出去的流上。只写名单的话，他会看着那个模型把整段话
   * 写完，然后觉得「按了没反应」。所以这里额外留一张便条：`#attempt` 每收到
   * 一个 chunk 就看一眼，命中就掐掉这一趟，名额交还，队列接着换人。
   *
   * 只活在内存里：它描述的是「此刻这一趟」的意图，重启之后没有任何调用在跑。
   *
   * @param {string|undefined} sessionId
   * @param {string} key
   */
  noteSkip(sessionId, key) {
    if (typeof sessionId !== 'string' || sessionId === '') return
    if (typeof key !== 'string' || key === '') return
    this.skipped.set(sessionId, { key, at: this.now() })
    // 顺手把卡在 `await nextChunk()` 里的那一趟叫醒：见 `skipWatchers` 上的注释。
    // 拷贝一份再遍历 —— 回调里可能把自己摘掉（`#attempt` 的 finally 就是这么做的）。
    const watchers = this.skipWatchers.get(sessionId)
    if (watchers === undefined) return
    for (const notify of [...watchers]) {
      try {
        notify(key)
      } catch {
        /* 叫醒失败不影响记账：下一轮检查还会再判一次。 */
      }
    }
  }

  /**
   * 挂一个「这个会话被跳过时叫我」的钩子，返回摘掉它的函数。
   *
   * @param {string|null|undefined} sessionId
   * @param {(key: string) => void} notify
   * @returns {() => void}
   */
  #watchSkip(sessionId, notify) {
    if (typeof sessionId !== 'string' || sessionId === '') return () => {}
    const existing = this.skipWatchers.get(sessionId)
    const set = existing ?? new Set()
    set.add(notify)
    this.skipWatchers.set(sessionId, set)
    return () => {
      set.delete(notify)
      if (set.size === 0 && this.skipWatchers.get(sessionId) === set) this.skipWatchers.delete(sessionId)
    }
  }

  /**
   * 这个候选是不是刚被用户点名跳过了（读一次就消费掉）。
   *
   * 消费是必须的：便条留着会让**下一个**用同一个候选的正常请求也被掐一次 ——
   * 那时隔离名单已经把它排除在队列外了，再掐一次纯属误伤。
   *
   * @param {string|undefined} sessionId
   * @param {string} key
   * @returns {boolean}
   */
  #skipPending(sessionId, key) {
    if (typeof sessionId !== 'string' || sessionId === '') return false
    const pending = this.skipped.get(sessionId)
    if (pending === undefined) return false
    if (pending.key !== key) return false
    this.skipped.delete(sessionId)
    return true
  }

  // ── 适配器契约 ─────────────────────────────────────────────────────────────

  providerInfo(provider) {
    return { id: provider, name: ROUTE_LABEL }
  }

  /**
   * 接力队列自己做重试与切换，所以这条路由对外声明「不做重试」。
   *
   * 否则 llm-retry 会把整条队列再跑一遍：每一轮都要等每个坏模型各自超时，
   * 延迟成倍放大，而队列本身已经把「所有候选都试过」这件事做完了。
   */
  providerRetryPolicy(_provider) {
    return { mode: 'normal', maxRetries: 0, retryableCodes: [NEVER_RETRY], ...DEFAULT_BACKOFF }
  }

  imageRequestPricing(_provider, _model) {
    return undefined
  }

  /**
   * 这条路由下的「模型」就是队列模板：一个模板一条队列。
   *
   * 用户在模型选择器里挑中哪个模板，那个会话就用哪条队列 —— 模板 id 会原样
   * 出现在 `GenerateOptions.model` 里，`stream()` 拿它选队列。
   */
  async listModels(provider) {
    const settings = this.settings.get()
    const templates = Array.isArray(settings.templates) ? settings.templates : []
    return templates.map((template) => ({
      provider,
      id: template.id,
      name: template.name,
      description: MODEL_DESCRIPTION,
      inputModalities: ['text', 'image'],
    }))
  }

  /**
   * `model` 就是模板 id。
   *
   * 找不到时退回第一个模板而不是报错：模板可能刚被删掉，而一个还挂着旧 id 的
   * 会话总得能继续跑下去。老版本里那条路由下只有 `auto` 一个模型，它也会落到
   * 这里，正好落到默认模板上。
   */
  templateFor(settings, modelId) {
    const templates = Array.isArray(settings.templates) ? settings.templates : []
    if (templates.length === 0) return undefined
    const hit = templates.find((template) => template.id === modelId)
    return hit ?? templates[0]
  }

  async resolveModel(provider, model, _signal) {
    const settings = this.settings.get()
    const template = this.templateFor(settings, model)
    const order = Array.isArray(template?.order) ? template.order : []
    const head = this.catalog.headContextWindow(order)
    return {
      provider,
      id: model,
      name: template?.name ?? model,
      description: MODEL_DESCRIPTION,
      // 图片交给真正的目标模型去判断：这里若声明成纯文本，宿主会在我的
      // stream() 之前就把图片压成文字，目标模型再也没机会用视觉能力。
      inputModalities: ['text', 'image'],
      // 对外声明的是**队首候选**的窗口（每个候选按自己的窗口接力）：
      // 由宿主提前压缩，好过在某个小窗口模型上撞 CONTEXT_WINDOW_EXCEEDED。
      // 换人落到更小的模型时，由宿主的 context-overflow 压缩路径兜底。
      ...head === undefined ? {} : { context: { contextWindow: head } },
      // **故意一个档位都不声明。**
      //
      // 这条路由背后是一串各有各档位表的模型，任何时候对外报一组档位都是在说谎：
      // 报并集，用户选了里面某一档，之后队列里带这一档的那个模型被删掉，宿主就会
      // 拿「这条路由不认识这个档位」把整轮拒掉（`UNSUPPORTED_REASONING_EFFORT`）；
      // 而档位表来自目录扫描，冷启动时还是空的 —— 同一个会话上一轮能过、下一轮就
      // 报错，用户看到的正是这种「有时候」。
      //
      // 所以档位这件事整体缩回队列内部：每个候选该用什么档位，在**设置页的队列行**
      // 上按它自己的表选（见 `#effortFor`），模型选择器里这条路由干脆没有档位可选，
      // 也就不存在「选了之后失效」这回事。
      //
      // 也故意不声明 defaultEffort：缺省档位应该由每个候选自己的适配器决定
      // （也就是它「正常使用」时的表现），这条路由替它选一个反而是越权。
    }
  }

  async prepareCall(provider, model, _signal) {
    const info = await this.resolveModel(provider, model)
    return {
      model: info,
      stream: (options) => this.stream(options),
    }
  }

  // ── 队列执行 ───────────────────────────────────────────────────────────────

  /**
   * 用户在模板里排的那条队列 —— **只做有效性过滤，不看「今天能不能用」**。
   *
   * 把这个从 `chain()` 里拆出来，是因为「谁排在谁前面」这件事不能拿过滤后的
   * 结果算：某个候选这会儿正被别的会话占着（于是从 `chain()` 里消失了），
   * 它却仍然是用户排在前面的那个 —— 它一被放出来，就该回到原位、而不是被
   * 当成新面孔排到队尾（见 `#freedCandidate`）。
   *
   * @returns {Array<{provider: string, model: string, reasoningEffort?: string, concurrency?: number}>}
   */
  #priority(settings, template) {
    const hidden = new Set(settings.hiddenProviders ?? [])
    const known = this.catalog.cached()
    const knownKeys = known.length === 0 ? undefined : new Set(known.map((item) => routeKey(item.provider, item.model)))
    const ordered = []
    for (const item of Array.isArray(template?.order) ? template.order : []) {
      if (ordered.length >= MAX_TEMPLATE_CANDIDATES) break
      if (item === null || typeof item !== 'object') continue
      const provider = String(item.provider ?? '')
      const model = String(item.model ?? '')
      if (provider === '' || model === '') continue
      if (provider === ROUTE || hidden.has(provider)) continue
      // 清单是冷的就不筛（fail-open）：宁可让宿主回一个 NO_ADAPTER 被跳过，
      // 也不要在目录还没扫完时把用户整条队列清空。
      if (knownKeys !== undefined && !knownKeys.has(routeKey(provider, model))) continue
      // 每个候选自带的思考强度跟着它一起走：换到下一个模型时，用的是**那个**
      // 模型自己的档位，不是上一个的。
      const reasoningEffort =
        typeof item.reasoningEffort === 'string' && item.reasoningEffort.trim() !== ''
          ? item.reasoningEffort.trim()
          : undefined
      // 这个候选自己的并发上限也跟着它走。它是**队内**的信息（选人时用），
      // 不会跟着请求发到上游去。
      const concurrency = concurrencyOf(item.concurrency)
      const entry = { provider, model }
      if (reasoningEffort !== undefined) entry.reasoningEffort = reasoningEffort
      if (concurrency !== undefined) entry.concurrency = concurrency
      ordered.push(entry)
    }
    return ordered
  }

  /**
   * 每个候选在用户队列里的位次（键 → 下标）。没设过的候选不在表里。
   *
   * @returns {Map<string, number>}
   */
  queueRank(settings, template) {
    const rank = new Map()
    this.#priority(settings, template).forEach((item, index) => {
      const key = routeKey(item.provider, item.model)
      if (!rank.has(key)) rank.set(key, index)
    })
    return rank
  }

  /**
   * 把设置里的顺序 + 当日隔离名单合成一条可执行的候选序列。
   *
   * 优先尝试「今天还没出过问题」的模型，保持用户排的优先级；全都出过问题
   * 时按用户设置决定是硬着头皮再试一遍还是直接放弃。
   *
   * @param {object} settings
   * @param {object | undefined} template - 这次调用用的模板；队列来自它的 `order`。
   * @param {{quarantined: Set<string>}} snapshot
   * @param {number} [skip] - 从队首跳过几个候选再开始（会按队列长度取模）。
   *   用于「同一个模型连着撞输出上限」时把队列往前挪一格换人接手。
   * @param {string} [sessionId] - 谁的调用。并发上限要按会话记账：自己占着的那
   *   一格不该把自己挡在门外，别人的占用才要绕开。
   */
  chain(settings, template, snapshot, skip = 0, sessionId) {
    const ordered = this.#priority(settings, template)
    // 总开关关掉时这条路由仍然能跑，只是退化成「永远用优先级最高的那一个」，
    // 不再重试、不再切换、也不再隔离 —— 排查问题时这样最干净。
    let candidates
    if (settings.enabled === false) {
      candidates = ordered.slice(0, 1)
    } else {
      // 跳过三类候选：今天已经隔离掉的、此刻还在限流冷却里的、以及**并发上限
      // 已经被别的会话占满**的。
      //
      // 前两者判据不同但效果一样 —— 这一轮先别用它。区别在**多久**：隔离要到
      // 明天，冷却只到几十秒后（见 lib/cooldown.js）。冷却表是内存态、不分
      // 会话，所以这里查一次就行；并发占用则要 sessionId —— 自己占着的那个
      // 候选仍然算可用（同一趟调用换账号重发时不该被自己挡在门外）。
      const cooling = this.cooldowns.keys()
      const usable = ordered.filter((item) => {
        const key = routeKey(item.provider, item.model)
        if (snapshot.quarantined.has(key)) return false
        if (cooling.has(key)) return false
        return this.limits.usable(key, item.concurrency, sessionId)
      })
      if (usable.length > 0) candidates = usable
      else {
        // 全都在冷却/隔离里：照旧看 `retryWhenAllQuarantined`。
        //
        // 这里**故意**把冷却也算进「全都不行」：用户把开关打开的意思是「就算
        // 全坏了也硬着头皮试一遍」，而不是「只有隔离的才试、冷却的不试」。
        candidates = settings.retryWhenAllQuarantined === false ? [] : ordered
      }
      // 手里攥着租约的那个候选排到队首（见 `#stickToLease`）。只在总开关打开时
      // 做：关掉总开关的意思是「永远用优先级最高的那一个」，让租约把它顶上来
      // 等于把开关废掉一半。
      candidates = this.#stickToLease(candidates, ordered, sessionId, snapshot)
    }
    if (!Number.isInteger(skip) || skip <= 0 || candidates.length < 2) return candidates
    // 取模是为了让队列真的转起来：跳过头了就从头接上，而不是原地不动。
    const at = ((skip % candidates.length) + candidates.length) % candidates.length
    if (at === 0) return candidates
    return [...candidates.slice(at), ...candidates.slice(0, at)]
  }

  /**
   * 这个会话已经占着名额的那个模型，排在队首 —— 除非它自己出了问题。
   *
   * 用户要的是「一个会话只要拿到了一个有并发上限模型的名额，就一直用它，直到
   * 这个模型报错或者会话结束」。座位只是账面上的占用，真正让用户看到「它一直
   * 在用这个模型」的，是这里：手里有租约的候选即使排在队列后面，这一趟也先
   * 用它，而不是按优先级飘到前面的别人身上。
   *
   * 三种情况不粘：
   *  1. 模板里已经没有它了（用户改了队列），或者它不再设并发上限了 —— 租约作废；
   *  2. 它被隔离了（今天用不上）—— 攥着只会让别人也进不来，一并作废；
   *  3. 用户没给它设上限 —— 没有租约可谈（`pinnedKey` 会返回 undefined）。
   *
   * 它被**限流冷却**挡住了不算「作废」：那是几十秒的临时状态，而且别的会话在同一
   * 个模型上撞的限流不该算到这个会话头上。等它回来，租约还在。
   *
   * 「同一个模型连着撞输出上限、该换人」这一条不在这里判：那是有意的轮换，
   * 由 `#runStream` 在开跑前把租约交还掉（见那里的注释）。
   */
  #stickToLease(candidates, ordered, sessionId, snapshot) {
    const pinned = this.limits.pinnedKey(sessionId)
    if (pinned === undefined) return candidates
    const meta = ordered.find((item) => routeKey(item.provider, item.model) === pinned)
    if (meta === undefined || meta.concurrency === undefined) {
      this.limits.free(pinned, sessionId, { cool: false })
      return candidates
    }
    if (snapshot?.quarantined?.has(pinned) === true) {
      this.limits.free(pinned, sessionId, { cool: false })
      return candidates
    }
    const at = candidates.findIndex((item) => routeKey(item.provider, item.model) === pinned)
    if (at <= 0) return candidates
    return [candidates[at], ...candidates.slice(0, at), ...candidates.slice(at + 1)]
  }

  /**
   * 把候选序列展开成执行步骤。
   *
   * 「继续」的条数 = 已经失败过的轮数：第 0 个模型第一次就是原始请求，之后
   * 每退让一步就多带一条「继续」—— 用户要求的「重试这个模型发继续」「切换到
   * 下一个模型发继续」都落在这一步上。
   */
  plan(settings, template, snapshot, skip, sessionId) {
    const chain = this.chain(settings, template, snapshot, skip, sessionId)
    const tries = sameModelTries(settings)
    const steps = []
    for (let index = 0; index < chain.length; index += 1) {
      // 退到队列里第 index 个模型时带几条「继续」：第 0 个是原始请求，每多换
      // 一次模型就多带一条 —— 用户要的「换下一个模型时发继续」落在这里。
      const continues = Math.min(index, MAX_CONTINUES)
      // 同一个模型的每次尝试发**完全相同的请求**。
      //
      // 一是为了换账号：只有重发原请求，`dsh-our-free-model` 这类多账号反代
      // 才会重新进一次账号池，把刚才那个坏账号换掉。二是新账号压根没写过东西，
      // 收到「继续」只会莫名其妙。
      for (let attempt = 0; attempt < tries; attempt += 1) {
        steps.push({ candidate: chain[index], continues })
      }
    }
    return steps
  }

  /**
   * 完整的一轮模型调用。
   *
   * 这一层只负责给「这一趟」记账（见 `Cooldowns#beginCall`）：冷却到点的模型要
   * 等替它跑的那一趟收工才能回队首，所以每一趟的开工/收工时刻都得有人记下来。
   * 真正的接力逻辑在 `#runStream()`。
   *
   * @param {any} options - 宿主给你的 GenerateOptions。
   */
  async * stream(options) {
    const call = this.cooldowns.beginCall()
    try {
      yield* this.#runStream(options)
    } finally {
      this.cooldowns.endCall(call)
    }
  }

  /**
   * 一轮调用的正身：排队、重发、换人、隔离。
   * @param {any} options - 宿主给你的 GenerateOptions。
   */
  async *#runStream(options) {
    const at = this.now()
    const settings = this.settings.get()
    // 隔离名单按会话分桶 —— 先把 sessionId 取出来，快照就要它。
    //
    // 压缩会话、起标题这类内部调用共用同一个 sessionId，它们**照样**进这个
    // 会话的桶（它们确实是在为这个会话干活，撞上的墙也是这个会话的墙）；
    // 但它们不参与「撞上限换人」的记账，因为那个账是给用户看的轮准备的。
    const sessionId = typeof options?.sessionId === 'string' && options.sessionId !== '' ? options.sessionId : undefined
    // 模型 id 就是模板 id。老会话里存的可能是 `auto`，`templateFor` 会把它接到
    // 第一个模板上，不至于因为升级而整条队列失效。
    const template = this.templateFor(settings, options?.model)
    const templateId = template?.id ?? DEFAULT_TEMPLATE_ID
    const snapshot = this.health.snapshot(at, settings.resetHour, sessionId, templateId)
    const userFacing = options?.purpose === undefined
    const streak = userFacing && sessionId !== undefined ? this.ceiling.get(sessionId) : undefined
    const skip = streak === undefined ? 0 : Math.floor(streak.hits / CEILING_STREAK_BEFORE_ROTATE)
    // sessionId 必须传给 `plan()`：并发名额是**会话级租约**，自己占着的那一格
    // 不该把自己挡在门外。不传的话每次调用都按「新会话」算，刚拿到手的名额
    // 立刻就被自己的账算成「满员」，于是永远用不上它。
    const steps = this.plan(settings, template, snapshot, skip, sessionId)

    if (steps.length === 0) {
      yield errorFinish(
        'NO_CANDIDATES',
        (Array.isArray(template?.order) ? template.order : []).length === 0
          ? `「模型接力」的模板「${template?.name ?? templateId}」还是空的：到 设置 → 模型 → 模型接力 里给它勾选并排序要用的模型。`
          : `模板「${template?.name ?? templateId}」里今天已经没有可用模型了（其余都被隔离），到会话页的「隔离队列」可以立刻解除这个会话的隔离。`,
      )
      return
    }

    const continueText = typeof settings.continueText === 'string' && settings.continueText.trim() !== ''
      ? settings.continueText
      : '继续'

    /**
     * 每次补「继续」之前先安静等这么久。
     *
     * 为什么要有这一步：紧贴着上一次失败再敲一遍，等来的几乎总是同一个错误。
     * 本机实测（2026-10-14 的事件日志）里最难看的一段是 **9 秒内落了 12 条隔离**，
     * 相邻两条只隔 0.0～1.3 秒 —— 限流还没来得及松、上游抖动还没过去，就又被戳了
     * 一遍，除了把队列挨个隔离掉没有任何收益。
     *
     * 第一次失败也照样等：`plan()` 里第一个候选的首跳 continues 是 0，只有失败之后
     * 才排得出带「继续」的那一跳，所以这一等正好落在「刚失败完、正要补继续」的
     * 那个瞬间，没有漏掉第一次。
     */
    const delayMs = Number.isInteger(settings.continueDelaySec) && settings.continueDelaySec > 0
      ? settings.continueDelaySec * 1000
      : 0

    /**
     * 多久没有任何 chunk 就算「卡住了」。
     *
     * 有些上游不报错也不吐字：连接挂着、响应头收完了，然后就再也没有然后。
     * 从这边看它跟「模型在很慢地思考」长得一模一样，只能靠时间判。0 = 不管。
     */
    const idleMs = Number.isInteger(settings.idleTimeoutSec) && settings.idleTimeoutSec > 0
      ? settings.idleTimeoutSec * 1000
      : 0

    const attempted = []
    let held = []
    let finish
    let failure
    /**
     * 上一跳是不是栽在**本机网络**上。
     *
     * 栽在网络上的话，后面几跳都不该再带「继续」：上一个模型连请求都没收到，没有
     * 任何「写到一半」的东西可以接着写，补一句「继续」只是往请求里灌噪音。
     *
     * 只改「带几条继续」，**不动**隔离与换人的判断。这里刻意不把「同模型重发一次」
     * 那一跳整个跳过 —— 跳过后 `markBroken` 就轮不到这个候选，它今天不会被隔离，
     * 下一个任务还会从它开始再撞一次，比多发一次请求更浪费。
     */
    let networkDown = false

    /**
     * 这一轮里已经被限流、刚排进冷却的候选。
     *
     * 用途只有一个：跳过它剩下的同模型重发。`plan()` 会给每个候选排
     * `sameModelRetries + 1` 步（为了换账号），但**正在限流的时候重发同一个
     * 请求只会让上游的队列更长** —— 那是要避免的动作，不是要做的动作。
     */
    const cooledHere = new Set()

    /**
     * 计划里每个候选各自排在第几（第一次出现的位置）。
     *
     * 只服务于下面那条「队首被放出来了」的判断：只有排在当前位置**前面**的候选
     * 才值得回头去用 —— 排在后面的插队就等于打乱用户排好的优先级。
     */
    // 「谁排在谁前面」按**用户排的那条队列**算，不按过滤后的计划算：某个候选
    // 可能因为正好被别的会话占着而从计划里消失了，但它原来的位次没变。
    const rank = this.queueRank(settings, template)
    /**
     * 这一轮已经**决定用过**的候选（真正打过，或者刚被「放出来了」捡回来）。
     *
     * 用途只有一个：别把同一个候选来回捡。已经试过的候选不该因为「它现在空闲」
     * 又被拉回来重试一遍 —— 那是打转，不是接力。
     */
    const decided = new Set()
    /** 上一个真正执行的候选，用来认出「换人的那一跳」。 */
    let lastKey
    let cursor = 0
    while (cursor < steps.length) {
      const step = steps[cursor]
      const { provider, model } = step.candidate
      const key = routeKey(provider, model)
      // 这一轮刚在它身上撞了限流：剩下的重发全部跳过，直接看下一个候选。
      if (cooledHere.has(key)) {
        cursor += 1
        continue
      }
      // 换人的那一跳：先看看队里靠前、设了并发上限的候选有没有被别的会话放出来。
      // 有就回头用它 —— 用户要的正是「别人一释放，这边就用回它」，而不是顺着
      // 往下走。同一个候选的重发（`key === lastKey`）不参与，免得来回打转。
      if (lastKey !== undefined && key !== lastKey && !decided.has(key)) {
        const freed = this.#freedCandidate(
          rank,
          rank.get(key),
          settings,
          template,
          snapshot,
          skip,
          sessionId,
          decided,
        )
        if (freed !== undefined) {
          decided.add(routeKey(freed.provider, freed.model))
          const tries = sameModelTries(settings)
          const inserted = []
          for (let attempt = 0; attempt < tries; attempt += 1) {
            inserted.push({ candidate: freed, continues: step.continues })
          }
          steps.splice(cursor, 0, ...inserted)
          this.log.warn(
            `model-failover: ${freed.provider}/${freed.model} 的并发空出来了，这一轮改用它`,
          )
          continue
        }
      }
      // 并发上限：换人之前它可能已经被别的会话占满了。占满了就把这一步（以及
      // 它剩下的重发）整体跳过 —— 它没坏，只是此刻轮不到我们，既不该隔离也
      // 不该冷却，等别的会话收工（或关掉会话）它自己就回来了。
      //
      // 但「刚有人用完、还在收工冷却里」是另一回事：那不是「别人占着」，而是
      // 这个候选自己刚干完活、正在歇。这时候先看看**后面还有没有别人能用**：
      // 有就跳过它（队列的意义就在这），没有就只能等这一小会儿 —— 否则整条
      // 队列会因为这个纯粹的节流而空转，用户看到的是「没模型可用」这种假故障。
      if (!this.limits.usable(key, step.candidate.concurrency, sessionId)) {
        const waitMs = this.limits.coolingMs(key)
        const later = waitMs > 0 && steps.slice(cursor + 1).some((item) => this.limits.usable(
          routeKey(item.candidate.provider, item.candidate.model),
          item.candidate.concurrency,
          sessionId,
        ))
        if (waitMs <= 0 || later) {
          cursor += 1
          continue
        }
        await this.sleep(waitMs, options?.signal)
        if (options?.signal?.aborted === true) {
          for (const chunk of held) yield chunk
          return
        }
        // 等完了还是不能用（别的会话抢先一步占了座）：那就老老实实往下走。
        if (!this.limits.usable(key, step.candidate.concurrency, sessionId)) {
          cursor += 1
          continue
        }
      }
      const continues = networkDown ? 0 : step.continues
      // 带「继续」的那一跳之前先等一等 —— 等的是上一次失败留下的余波。
      // 不带的跳（每个候选的首发）不欠这一等，否则第一次调用就会平白慢 5 秒。
      if (continues > 0 && delayMs > 0) {
        await this.sleep(delayMs, options?.signal)
        // 等待期间用户按了停止：别再打一个注定被取消的请求。
        if (options?.signal?.aborted === true) {
          for (const chunk of held) yield chunk
          return
        }
      }

      const messages = continues === 0
        ? options.messages
        : [...options.messages, ...Array.from({ length: continues }, () => continueMessage(continueText))]

      const effort = this.#effortFor(step.candidate, options?.reasoningEffort)
      // 标记「此刻正在跑的就是这一跳」，会话页据此把这一栏刷得快一点、并把还没
      // 跑完任何一趟的会话也列出来。只包住真正的请求：上面等待和下面记账都不算
      // 「正在跑」，否则用户会看到一行永远亮着的「运行中」。
      if (sessionId !== undefined) {
        this.live.set(sessionId, { provider, model, key, startedAt: this.now(), templateId })
      }
      // 占住这个候选的一格并发。没设上限的候选不记账（`hold` 直接返回 false）。
      //
      // 这是**会话级租约**，不是请求级占用：拿到手就钉在这儿，这一轮里换账号重发、
      // 补「继续」、写完整轮都不放手，下一趟调用也不再重新抢一次。只有它报错了
      // （下面隔离分支里的 `free`）或者会话结束了（index.js 监听
      // `session/disposed` 调 `freeSession`）才交还。
      //
      // 早先是每发一次请求 acquire、收工立刻 release，等于每跳之间都把座位让出去
      // + 顺手触发一笔收工冷却，会话于是被自己的账推着「时不时放弃这个模型去用
      // 下一个」—— 那正是用户报的问题。
      const cap = step.candidate.concurrency
      this.limits.hold(key, cap, sessionId)
      /**
       * 交还这一趟占着的名额。
       *
       * 只在三种时候调：这个模型**报错**了（用户原话就是「直到这个模型报错」）、
       * 会话结束了（`index.js` 那边调 `freeSession`）、或者它被移出了模板。
       * 正常写完、撞上限接着写，都**不**交还 —— 那正是「一直占着」的意思。
       *
       * `cool` 决定这次交还要不要记一笔「交还冷却」（设置页那个默认 5 秒）。
       * 判据是**下一个人真的会因此受益吗**：
       *  - 它刚写完一段、因为撞上限轮换给别人 → 要记，别人马上顶进去只会撞出 429；
       *  - 它被限流了 → 不记，限流冷却表已经在拦着它，再叠一笔只会更晚回来；
       *  - 它今天隔离了 → 不记，本来就不再接客，挂个「再歇 5 秒」纯属噪音。
       */
      const dropLease = (cool = true) => {
        if (cap === undefined) return
        this.limits.free(key, sessionId, { cool })
      }
      let result
      try {
        result = yield * this.#attempt(step.candidate, options, messages, effort, idleMs)
      } finally {
        if (sessionId !== undefined) this.live.delete(sessionId)
      }
      attempted.push({ provider, model, continues })
      decided.add(key)
      lastKey = key
      // 记下「这个会话刚才跑的是谁」，供轮级那层在 turn/end 时与轮号配对。
      //
      // 每跳都覆盖是有意的：一轮里可能连着换好几个候选，最后写进来的那个才是
      // 真正产出内容（或截断在半句话上）的模型；而候选一旦提交内容，下面立刻
      // 就 return，不会再有人覆盖它。
      //
      // 压缩会话、起标题这类**不是用户在等**的调用要排除：它们共用同一个
      // sessionId，却和用户看到的这一轮无关，会把便签指到错的人身上。
      if (options?.purpose === undefined) {
        this.notes?.remember(options?.sessionId, { provider, model, template: templateId, at: this.now() })
      }
      held = result.held
      finish = result.finish
      failure = result.failure
      networkDown = failure !== undefined && isLocalNetworkFailure(failure)
      // 输出速度记在会话页那一栏的胶囊上。和上面那条便签同样的理由排除内部
      // 调用：起标题只吐十几个字，量出来的速度没有参考价值。
      if (userFacing && sessionId !== undefined && result.speed !== undefined) {
        this.health.recordSpeed(sessionId, key, { ...result.speed, at: this.now() })
      }

      if (result.committed) {
        // 这一轮正常写完了：撞上限的旧账一笔勾销，下一个任务还从队首开始。
        if (userFacing && sessionId !== undefined) this.ceiling.delete(sessionId)
        // 它刚刚产出了内容 —— 那就更不可能是「还在限流里」，把冷却摘掉。
        // 留着只会让队列在接下来几十秒里白白绕开一个明明能用的模型。
        this.cooldowns.delete(key)
        // 这个模型今天被验证过能用了；下一个任务从它开始。
        const recorded = this.health.markGood(key, this.now(), sessionId, templateId)
        this.health.file.flush()
        if (recorded) this.log.push({ at: this.now(), kind: 'good', provider, model, attempts: attempted.length })
        return
      }

      // 用户点了「跳过」：这一趟的隔离账在 `/skip` 那一刻就写好了（见
      // `skip()`），这里**只**负责别把这这一趟的东西交出去，然后接着换人。
      //
      // 三个「不」都是有意的：
      //  - 不再 `markBroken` 一次：那会把同一条记录写成「模型自己报错」，用户
      //    看着就是自己按的按钮变成了上游的错误；
      //  - 不 `yield held`：它憋了半天却没提交任何内容，交出去只会让调用方
      //    拿到一半的东西然后收工 —— 用户按跳过是为了**接着往下跑**，不是结束；
      //  - 不 `return`：队列里还有别人，接力就是别停在这一格。
      if (result.skipped === true) {
        // 这一趟的「跳过」账在 `skip()` 那边已经记了（连 `live` 一起），这里
        // **不再**补一条：同一件事写两行，事件日志看着像按了两次。上面那行注释
        // 里的三个「不」才是这里要负责的。
        dropLease(false)
        cursor += 1
        continue
      }

      // 撞到输出上限：**不是**「这个模型坏了」，是「它还没写完」。
      //
      // 必须在这里就把扣住的东西原样交出去并收手，绝不能让它掉进下面的隔离分支
      // —— 那正是用户报的问题：模型一到 token 上限就被隔离，下一轮接力从别人
      // 开始，「继续」再也落不回原来那个模型头上。
      //
      // 交出去的 finish 仍带着 `kind: 'max-tokens'`，agent loop 会把它记成
      // `turn/end { kind: 'max-tokens' }`（dsh-agent-loop lib/index.js:1151 判定、
      // :1027 落事件），轮级的「自动继续」随即用**同一个模型**接着写。
      //
      // 但同一个模型连着撞就不该再惯着它（见 CEILING_STREAK_BEFORE_ROTATE）：
      // 记一笔账，攒够了下一次调用就从队里的下一个开始。
      //
      // 放在「中止」判断之前还是之后无所谓：两者都是把同一个 `held` 原样交出去
      // 然后 return，顺序不影响任何可观察行为。
      if (result.truncated) {
        if (userFacing && sessionId !== undefined) {
          // 账按**会话**累加，不按模型：接力换过人之后账要接着往下数，否则下一
          // 次调用又会回到队首、在原来那个模型上再撞两下，来回打转。
          const previous = this.ceiling.get(sessionId)
          const hits = (previous?.hits ?? 0) + 1
          this.ceiling.set(sessionId, { hits, key, provider, model })
          // 攒够一轮就该换人了：下一次调用 `skip` 加一，队列从下一个候选开始。
          // 日志里说清楚，否则用户只看到排在最前面的那个模型莫名其妙被跳过。
          if (hits % CEILING_STREAK_BEFORE_ROTATE === 0) {
            const next = this.chain(settings, template, snapshot, hits / CEILING_STREAK_BEFORE_ROTATE, sessionId)[0]
            if (next !== undefined && routeKey(next.provider, next.model) !== key) {
              this.log.push({
                at: this.now(),
                kind: 'ceiling-rotate',
                provider,
                model,
                to: `${next.provider}/${next.model}`,
                count: CEILING_STREAK_BEFORE_ROTATE,
              })
            }
            // 攒够了就该轮到别人了：租约也一起交还，否则下一次调用会因为
            // 「手里还攥着它」把它重新排回队首，用户设的轮换等于没发生。
            // 这里**要**记交还冷却：它刚吐完一大段，上游那边的 token 核算还没
            // 落账，下一个会话立刻顶进去很容易撞出个跟模型质量无关的 429。
            dropLease(true)
          }
        }
        for (const chunk of result.held) yield chunk
        return
      }

      // 用户自己按了停止：换模型救不回来，把真实结局原样交出去。
      if (options?.signal?.aborted === true || failure?.code === 'ABORTED') {
        for (const chunk of held) yield chunk
        return
      }

      // 请求本身的问题（上下文超长之类）：换模型只会再撞一次墙。
      if (failure !== undefined && NON_FAILOVER_CODES.has(failure.code)) {
        // 上游把「这一坨太大」写在 HTTP 400/413/422 或自定义文案里（见
        // `looksLikeContextOverflow`）。宿主要先认得出这个码，才肯走它自己的
        // 压缩路径；直接把它那句 422 原样交回去，agent loop 只当成普通报错。
        if (failure.code === 'CONTEXT_WINDOW_EXCEEDED' && finish?.reason?.failure?.code !== 'CONTEXT_WINDOW_EXCEEDED') {
          for (const chunk of held) {
            if (chunk?.type !== 'finish') yield chunk
          }
          yield errorFinish('CONTEXT_WINDOW_EXCEEDED', failure.message)
          this.log.warn(`model-failover: ${provider}/${model} 的请求超出上下文窗口，已改判交给宿主压缩（上游原话：${failure.code}）`)
          return
        }
        for (const chunk of held) yield chunk
        this.log.warn(`model-failover: ${provider}/${model} 以不可切换的错误结束 (${failure.code})`)
        return
      }

      // 限流：**冷却**，不是隔离。
      //
      // 上游限流的意思是「现在排队的太多，过几秒再来」，它自己都会在报错里
      // 写清楚什么时候恢复（线上实例：`~5s, exponential backoff, auto-resumes
      // when the limit clears`）。把这种失败记成「这个模型今天出不了东西」，
      // 用户看到的就是「一次限流就被踢出队列，第二天才回来」—— 明明还能用。
      //
      // 所以这里只让它歇一会儿：写进内存里的冷却表（不分会话、不落盘），
      // 队列接下来绕开它，到点自动回来，隔离名单上一条都不留。
      //
      // 放在「同模型重发」之前是**故意的**：正在限流的时候把同一个请求再发
      // 一遍，只会让它的队列更长 —— 那正是要避免的动作。
      //
      // `cooldownSec === 0` 是「关掉冷却」，限流回到老行为（隔离到明天）——
      // 这个 0 有意义，不能当成「没设」回落到默认值。
      const cooldownSec = Number.isInteger(settings.cooldownSec) ? settings.cooldownSec : 60
      if (failure !== undefined && cooldownSec > 0 && isTransientFailure(failure)) {
        const ms = cooldownMsFor(failure, this.now(), cooldownSec * 1000, MAX_COOLDOWN_SEC * 1000)
        this.cooldowns.put(key, {
          provider,
          model,
          code: failure.code,
          message: failure.message,
          ms,
          at: this.now(),
        })
        // 这一趟剩下的同模型重发也一并跳过：它还在限流里，重发没有意义。
        cooledHere.add(key)
        this.log.push({
          at: this.now(),
          kind: 'cooldown',
          provider,
          model,
          sessionId,
          templateId,
          code: failure.code,
          message: failure.message ?? '',
          ms,
          continues,
        })
        this.log.warn(
          `model-failover: ${provider}/${model} 被限流 (${failure.code})，先绕开 ${Math.round(ms / 1000)} 秒，不算今天的隔离`,
        )
        // 它自己被上游顶回来了 —— 这就是「这个模型报错」，租约到此为止，让别的
        // 会话去试（它们有各自的账号池）。不记收工冷却：限流冷却表已经管着这
        // 段时间了，再叠一笔只会让空出来的名额更晚才有人用。
        dropLease(false)
        cursor += 1
        continue
      }

      // 这个候选还有一次「继续」没用完：先别判它今天不行。
      //
      // 隔离的语义是「今天它出不了东西」，而这要等它的重试都用完才能下结论：
      // 失败一次、被一句「继续」叫起来接着写完的模型是**能用**的，把它隔离掉
      // 反而会让今天的任务从这里绕开一个明明好好的模型。只有队列里的下一个
      // 步骤换人了（或者没有下一步了），才说明这个候选这次真的交不出东西。
      const next = steps[cursor + 1]
      if (next !== undefined && next.candidate.provider === provider && next.candidate.model === model) {
        cursor += 1
        continue
      }

      // 这个候选今天出了问题 —— 隔离到下一个逻辑日，后面的任务不会再从它开始。
      this.health.markBroken(key, {
        code: failure?.code,
        message: failure?.message,
        at: this.now(),
        sessionId,
        templateId,
      })
      this.health.file.flush()
      this.log.push({
        at: this.now(),
        kind: 'quarantine',
        provider,
        model,
        // 事件日志里也带上会话：隔离是按会话算的，没有这个字段就分不清某一行
        // 是哪个会话撞的墙。模板同理 —— 一个模板一条队列。
        sessionId,
        templateId,
        code: failure?.code ?? CODE_NO_VISIBLE_OUTPUT,
        message: failure?.message ?? '',
        // 如实记录这一跳**实际**带了几条「继续」—— 本机网络没通时它是 0，
        // 而不是 plan() 里排的那个数。事件日志是排查问题的依据，不能写计划值。
        continues,
      })
      this.log.warn(
        `model-failover: ${provider}/${model} 无法输出 (${failure?.code ?? CODE_NO_VISIBLE_OUTPUT})，隔离到明天 ${settings.resetHour} 点`,
      )
      // 真出不了东西了 —— 「直到这个模型报错」里的那种报错，名额交还。
      dropLease(false)
      cursor += 1
    }

    // 队列走完了。把最后一轮扣住的东西交出去，让用户看见真实报错。
    for (const chunk of held) yield chunk
    if (finish === undefined) {
      yield errorFinish(
        failure?.code ?? CODE_NO_VISIBLE_OUTPUT,
        failure?.message ?? '队列里的模型都没能产出内容。',
        attempted,
      )
    }
  }

  /**
   * 换人的那一跳上，队里靠前的某个**设了并发上限**的候选是不是刚被别的会话放出来了。
   *
   * 这是用户需求里那半句话的落点：「使用其他模型的会话每次切换模型时检测队列中这些
   * 设置了并发上限的模型有没有被其他会话释放出来，如果有就使用这个被释放的模型」。
   *
   * 三条限定，缺一不可：
   *
   *  1. **只认设了上限的候选。** 没设上限的候选从来不会被占用挡住，也就不存在
   *     「被释放」这回事 —— 它们本来就一直在队列里按优先级参与。
   *  2. **只认排在当前位置前面的。** 排在后面的插队等于打乱用户排好的优先级，
   *     那不是「回到被释放的模型」，那是抢跑。
   *  3. **只认这一轮还没决定用过的。** 已经试过的候选现在空闲了也不该被拉回来：
   *     它刚才就是在这儿失败的，再试一遍只是打转。
   *
   * 返回 undefined 表示「没有值得回头的」，调用方照原计划往下走。
   *
   * @param {Map<string, number>} rank 计划里每个候选第一次出现的位置
   * @param {number|undefined} ownRank 当前这一步在计划里的位置
   * @returns {{provider: string, model: string, reasoningEffort?: string, concurrency?: number}|undefined}
   */
  #freedCandidate(rank, ownRank, settings, template, snapshot, skip, sessionId, decided) {
    if (ownRank === undefined) return undefined
    // 重新问一次队列：`chain()` 里的并发过滤是按**此刻**的占用算的，刚刚被
    // 释放出来的候选会因为这一问而重新出现在它原来的位置上。
    for (const item of this.chain(settings, template, snapshot, skip, sessionId)) {
      const key = routeKey(item.provider, item.model)
      // 排在当前位置之后的不算「回头」—— 捡它等于插队，那是抢跑不是接力。
      // 队列被 `skip` 转过时顺序不是单调的，所以这里跳过而不是收手。
      const at = rank.get(key)
      if (at === undefined || at >= ownRank) continue
      if (item.concurrency === undefined) continue
      if (decided.has(key)) continue
      if (!this.limits.usable(key, item.concurrency, sessionId)) continue
      return item
    }
    return undefined
  }

  /**
   * 这一轮该给这个候选带什么思考强度。
   *
   * 规则只有一条：**只发这个模型自己声明支持的档位**。候选们各有各的档位表，
   * 把一个它不认识的枚举值发下去，它的适配器多半会以 UNSUPPORTED_REASONING_EFFORT
   * 拒绝整个请求 —— 那等于把一个本来好好的模型判成「今天出不了东西」而隔离掉。
   *
   * 认不出来（还没问到档位表，或确实不支持）就不发，让目标模型用自己的默认
   * 档位 —— 也就是它平时的表现。
   *
   * 优先级：队列里给这个模型设的档位 > 调用方给「模型接力」这条路由选的档位。
   * 前者是这个模型的专属选择，后者是「这一轮大家统一用哪个档位」的意图，所以
   * 用户为某个模型明确设过的值不会被它盖掉；反过来，没设过的模型会跟着路由
   * 的档位走，而不是默默用默认档位。
   *
   * 注意 `requested` 这一路现在基本是空的：这条路由在 `resolveModel` 里一个档位
   * 都不声明，宿主就不会替它选、更不会发下来（见那里的注释）。留着是为了
   * 别再让一个「明明能过」的请求因为档位被整轮拒掉 —— 这个判断本身没有代价。
   */
  #effortFor(candidate, requested) {
    const known = this.catalog.reasoningFor(candidate.provider, candidate.model)
    const supported = (value) => known !== undefined && known.efforts.some((effort) => effort.id === value)
    const configured =
      typeof candidate.reasoningEffort === 'string' && candidate.reasoningEffort !== ''
        ? candidate.reasoningEffort
        : undefined
    if (configured !== undefined) {
      if (supported(configured)) return configured
      this.log.warn(
        `model-failover: ${candidate.provider}/${candidate.model} 不认识档位「${configured}」，这一轮改用它的默认档位`,
      )
      return undefined
    }
    const wanted = typeof requested === 'string' && requested !== '' ? requested : undefined
    return wanted !== undefined && supported(wanted) ? wanted : undefined
  }

  /**
   * 一次候选尝试：把 chunk 拉进来，扣住，直到出现提交点才放行。
   *
   * 返回 `{ committed, truncated, finish, failure, held, speed }`。`held` 在未提交时
   * 交还给调用方 —— 队列还有下一个候选就丢掉它，队列走完了就原样放出去。
   * `speed` 是这一趟的输出速度（`{ tokens, ms }`），没跑出东西时是 `undefined`。
   *
   * `truncated` 单独拎出来，是因为「撞到输出上限」既不是成功也不是失败：
   * 见下面 `failureOf` 之前的注释。
   *
   * `idleMs` 是「多久没有任何 chunk 就算卡住」；0 表示不管。
   */
  async * #attempt(candidate, options, messages, effort, idleMs = 0) {
    const nested = { ...options, provider: candidate.provider, model: candidate.model, messages }
    // 调用方给「模型接力」这条路由选的档位不能原样透给候选：它是在队列的并集上
    // 校验过的，候选未必认识。这一轮该不该带档位由 `#effortFor` 决定，所以这里
    // 显式删掉，避免它顺着 `...options` 漏下去。
    if (effort === undefined) delete nested.reasoningEffort
    else nested.reasoningEffort = effort
    let iterator
    try {
      iterator = this.llm.stream(nested)[Symbol.asyncIterator]()
    } catch (error) {
      return { committed: false, truncated: false, skipped: false, finish: undefined, failure: failureOf(undefined, error), held: [], speed: undefined }
    }

    const held = []
    let committed = false
    let finish
    let failure
    let drained = false
    /** 这一趟是被「太久没有输出」掐掉的。收尾时据此决定要不要等 return()。 */
    let idled = false
    /**
     * 这一趟是被用户点「跳过」掐掉的。
     *
     * 和 `idled` 一样是「不是上游的错」的收尾方式，但走的账不同：跳过的隔离在
     * `/skip` 那一刻就已经写进 `health` 了，这里只是把这一趟**提前**结束，别让
     * 它把整段话写完。所以外面的循环看到这个标记时不该再记一次隔离。
     */
    let skipped = false
    // 输出速度的两个端点：第一个真的在写字（或想事情）的 chunk，和收到 finish
    // 的那一刻。**不含**排队与首字延迟 —— 那是延迟，不是速度，混进来会让一个
    // 「想很久但写得很快」的推理模型显得很慢。
    let firstDeltaAt
    let finishedAt
    let outputTokens

    /**
     * 上一次收到 chunk 的时刻。
     *
     * 计时从**发请求**那一刻起算（不是从第一个 chunk 起算）：连接挂住、连响应头
     * 都不回的那种，恰恰是最该被判成卡住的一种。之后每来一个 chunk 都拨回来 ——
     * 思考增量也算，只要它还在动就不算卡住。
     */
    let lastAt = this.now()

    /**
     * 等下一个 chunk，最多等 `idleMs`；顺便和「用户点了跳过」赛跑。
     *
     * 为什么不是简单套一层 `Promise.race` 了事：超时之后那个 `iterator.next()`
     * 还挂着，它会晚些时候 resolve/reject。这里给它挂一个空 catch，避免变成
     * unhandledRejection；真正的收尾交给外面的 `iterator.return()`。
     *
     * 返回值三选一：`{ row }`（来东西了）、`{ idle: true }`（超时）、
     * `{ skip: true }`（用户按了跳过）。`idleMs` 为 0 时也要和跳过赛跑 ——
     * 「不判卡住」不等于「不许人叫停」。
     */
    const nextChunk = async () => {
      const pending = iterator.next()
      pending.catch(() => {})
      const racers = [pending.then((row) => ({ row })), skipSignal.then(() => ({ skip: true }))]
      let timer
      if (idleMs > 0) {
        const remaining = lastAt + idleMs - this.now()
        if (remaining <= 0) return { idle: true }
        racers.push(new Promise((resolve) => {
          timer = setTimeout(() => resolve({ idle: true }), remaining)
          // 别让这个定时器把进程吊着（离线测试、CLI 退出时会踩到）。
          timer?.unref?.()
        }))
      }
      try {
        return await Promise.race(racers)
      } finally {
        clearTimeout(timer)
      }
    }

    /**
     * 「用户点了跳过」这个信号本身，做成一个能参与 `Promise.race` 的东西。
     *
     * 单次就够：一旦跳过，下面立刻 break，不会再等第二次。
     */
    const selfKey = routeKey(candidate.provider, candidate.model)
    let wakeSkip
    const skipSignal = new Promise((resolve) => { wakeSkip = resolve })
    const unwatch = this.#watchSkip(options?.sessionId, (key) => {
      if (key === selfKey) wakeSkip()
    })

    try {
      for (;;) {
        // 开工前先看一眼：上一跳收工时用户就可能已经按下跳过了（`/skip` 到达
        // 得比这一跳早），那样就不该再白打一次请求。
        if (this.#skipPending(options?.sessionId, selfKey)) {
          skipped = true
          failure = { code: CODE_MANUAL_SKIP, message: `用户跳过了 ${candidate.provider}/${candidate.model}。` }
          break
        }
        let row
        let jumped = false
        try {
          const step = await nextChunk()
          if (step.idle === true) idled = true
          else if (step.skip === true) jumped = true
          else row = step.row
        } catch (error) {
          drained = true
          failure = failureOf(undefined, error)
          break
        }
        if (jumped) {
          // 用户在上游憋着的时候按了跳过：立刻收手。这一趟的隔离账 `/skip` 那边
          // 已经写好了，这里只负责结束它、并把 `skipped` 带出去。
          //
          // 和 `idled` 一样**不等** `iterator.return()`（见 finally 里的说明）：
          // 上游正挂在那儿不动，等它就是把「按下按钮」变成「再等五分钟」。
          // 顺手把那张便条消费掉，免得下一个候选被同一个信号误伤。
          this.skipped.delete(options?.sessionId)
          skipped = true
          failure = { code: CODE_MANUAL_SKIP, message: `用户跳过了 ${candidate.provider}/${candidate.model}。` }
          break
        }
        if (idled) {
          // 超时这一跳不算「下游出错」：把结局写成一次正常失败，让上面那套
          // 隔离/换人逻辑照常处理 —— 卡住的模型和报错的模型，对用户是一回事。
          failure = {
            code: CODE_IDLE_TIMEOUT,
            message: `${candidate.provider}/${candidate.model} 超过 ${Math.round(idleMs / 1000)} 秒没有任何输出，判定为卡住。`,
          }
          break
        }
        lastAt = this.now()
        if (row.done === true) {
          drained = true
          break
        }
        const chunk = row.value
        if (chunk !== null && typeof chunk === 'object') {
          if (chunk.type === 'finish') {
            finish = chunk
            finishedAt = this.now()
          } else if (chunk.type === 'usage') {
            if (Number.isFinite(chunk.usage?.outputTokens)) outputTokens = chunk.usage.outputTokens
          } else if (
            firstDeltaAt === undefined &&
            (chunk.type === 'text-delta' || chunk.type === 'reasoning-delta' || chunk.type === 'tool-call-delta')
          ) {
            firstDeltaAt = this.now()
          }
        }
        if (committed) {
          yield chunk
          continue
        }
        held.push(chunk)
        if (commitsAnswer(chunk)) {
          committed = true
          for (const pending of held) yield pending
          held.length = 0
        }
      }
    } finally {
      // 叫醒钩子必须摘掉：它按会话号存在 Map 里，会话不结束就不会自己消失。
      unwatch()
      // 消费方提前中断（用户按停止）时必须显式向下游传播 return()，否则上游
      // HTTP 流会继续跑完 —— 供应商照常计满 token，chunk 却没人接。
      if (!drained) {
        const closing = (() => {
          try {
            return iterator.return?.()
          } catch {
            return undefined
          }
        })()
        if (idled || skipped) {
          // 超时 / 跳过这一跳**不能等** return() 落地：下游正挂在一次没完没了的
          // 读上，return() 要等那个 await 自己回来才轮得到，等于把「按下按钮」
          // 又变回「等到天荒地老」。发了就当收工，它的结果（或异常）没人要。
          void Promise.resolve(closing).catch(() => {})
        } else {
          try {
            await closing
          } catch {
            /* 关闭失败无关紧要 */
          }
        }
      }
    }

    // 撞到输出上限（`finish.reason.kind === 'max-tokens'`）**不是**「这个模型坏了」。
    // 它明明在写，只是被截断在半句话上。判据必须放在 failureOf 之前：否则它会被
    // 收敛成 `NO_VISIBLE_OUTPUT`，再被当成「今天它出不了东西」隔离掉。
    //
    // 线上事件日志（`$DSH_HOME/dsh-model-failover/log.json`）里 13 条
    // `NO_VISIBLE_OUTPUT` 隔离，去重后**只有一条**原文：
    // 「模型以 max-tokens 结束，但整轮没有产出任何可见内容。」—— 全部出自这一条
    // 路径。模型被冤枉，而下一轮接力的起点也从此换成了别人，于是用户看到的正是
    // 「撞了上限之后，插件不再用原来那个模型补『继续』」。
    const truncated = !committed && finish?.reason?.kind === 'max-tokens'
    if (!committed && failure === undefined && !truncated) failure = failureOf(finish, undefined)
    // 速度只在「这一趟真的收到了 finish，并且吐过 token」时才算数：中途报错、
    // 一个 token 都没吐、或者根本没有 usage 的供应商，都不该在界面上留下一个
    // 假的数字。
    const speed =
      firstDeltaAt !== undefined &&
      finishedAt !== undefined &&
      finishedAt > firstDeltaAt &&
      Number.isFinite(outputTokens) &&
      outputTokens > 0
        ? { tokens: outputTokens, ms: finishedAt - firstDeltaAt }
        : undefined
    return {
      committed,
      truncated,
      skipped,
      finish,
      failure: committed || truncated ? undefined : failure,
      held: committed ? [] : held,
      speed,
    }
  }
}

/** 合成一个终止用的 finish chunk。 */
export function errorFinish(code, message, attempted) {
  return {
    type: 'finish',
    reason: {
      kind: 'error',
      failure: {
        code,
        message: attempted === undefined || attempted.length === 0
          ? message
          : `${message}（已尝试：${attempted.map((row) => `${row.provider}/${row.model}`).join(' → ')}）`,
      },
    },
  }
}
