/**
 * 「过一会儿自己会好」的失败 —— 冷却，而不是隔离一整天。
 *
 * 隔离（`lib/health.js`）的语义是「这个模型今天出不了东西」：额度耗尽、凭证
 * 失效、被上游封了。那种失败重试一百次也是同一个结局，所以要等到下一个逻辑日。
 *
 * 限流完全不是这回事。上游的原话（2026-11-04 线上事件日志，raccoon）：
 *
 *   raccoon: upstream rate-limited 4 consecutive request(s) to xiaohuanxiong.com;
 *   bili is holding outbound sends until 2026-11-04T08:44:44.803Z (~5s,
 *   exponential backoff) ... auto-resumes when the limit clears
 *
 * 它自己都写着 `~5s` 之后恢复。把这种失败塞进「隔离到明天 1 点」，等于因为
 * 一次五秒钟的排队，让这个模型今天一整天都不再被使用 —— 用户看到的就是
 * 「明明还能用的模型，一次限流就被踢出队列，第二天才回来」。
 *
 * 所以这里单开一档：**冷却**。冷却只让队列在接下来这一小段时间里绕开它，
 * 到点自动回来，不写进当日的隔离名单，也不影响别的会话。冷却记录只活在内存里
 * —— 它最多活几分钟，跨进程重启保留它没有任何价值，重启一次就当没事发生。
 *
 * 判据仍然遵循 `network.js` 立的规矩：**宁可漏判，不要误判**。拿不准的失败
 * 继续走隔离那条路（隔离一天最多让人少用一个模型，误冷却会让队列反复撞同一堵
 * 墙），所以这里只认非常明确的限流信号。
 */

/** 错误码层面的「忙，不是坏」。 */
export const TRANSIENT_CODES = new Set(['RATE_LIMIT'])

/**
 * 报错文本里的限流信号。
 *
 * 为什么必须有文本这一路：线上的限流经常不是以 `RATE_LIMIT` 这个码到达的 ——
 * 有的适配器把它包成 `SERVER`（HTTP 429 落在 4xx/5xx 的兜底分支里），有的
 * 直接在 message 里写一句 `upstream rate-limited`，code 却是 `UNKNOWN`。
 * 只看码会漏掉一大半。
 */
export const TRANSIENT_PATTERNS = [
  /\brate[- ]?limit(?:ed|ing)?\b/i,
  /\brate[- ]?limit\b/i,
  /\btoo many requests\b/i,
  /\bthrottl(?:ed|ing)\b/i,
  /\b429\b/,
  /\bconcurrent(?:ly)? requests?\b/i,
]

/**
 * 这条失败是不是「过一会儿自己会好」。
 *
 * @param {{code?: string, message?: string} | undefined} failure
 * @returns {boolean}
 */
export function isTransientFailure(failure) {
  if (failure === undefined || failure === null) return false
  const code = typeof failure.code === 'string' ? failure.code : ''
  if (TRANSIENT_CODES.has(code)) return true
  const message = typeof failure.message === 'string' ? failure.message : ''
  if (message === '') return false
  return TRANSIENT_PATTERNS.some((pattern) => pattern.test(message))
}

/**
 * 从报错文本里读上游自己说的「什么时候再来」。
 *
 * 按可信度从高到低认三种写法，全都是线上真实出现过的：
 *
 * 1. `until 2026-11-04T08:44:44.803Z` —— 绝对时刻，最准，直接用；
 * 2. `retry after 20s` / `retry-after: 20` / `try again in 30 seconds`；
 * 3. `(~5s, exponential backoff)` —— 括号里的相对秒数。
 *
 * 读不出来就返回 undefined，交给调用方用设置里的默认值。**不猜**。
 *
 * @param {string} message
 * @param {number} now
 * @returns {number | undefined} 毫秒数（相对 now）
 */
export function retryAfterFromMessage(message, now) {
  if (typeof message !== 'string' || message === '') return undefined

  // 1. 绝对时刻。只认 ISO 8601 那种带 T 和时区的，裸的 "2026-11-04" 太容易
  //    误伤（日志里到处都是日期），而且它也不像「什么时候恢复」。
  const at = message.match(/\buntil\s+(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2}))/i)
  if (at !== null) {
    const stamp = Date.parse(at[1])
    if (Number.isFinite(stamp) && stamp > now) return stamp - now
  }

  // 2. 显式的 retry-after。
  const explicit = message.match(/\b(?:retry[- ]after|try again in|retry in)\b[^\d]{0,8}(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|secs?|seconds?|m|mins?|minutes?)?/i)
  if (explicit !== null) {
    const value = Number.parseFloat(explicit[1])
    if (Number.isFinite(value) && value > 0) return scale(value, explicit[2])
  }

  // 3. 括号里的 `~5s` / `5 seconds`。放宽到全文，但要求带单位，否则
  //    「4 consecutive request(s)」里的 4 会被读成四毫秒。
  const relative = message.match(/[~≈]?\s*(\d+(?:\.\d+)?)\s*(ms|milliseconds?|s|secs?|seconds?|m|mins?|minutes?)\b/i)
  if (relative !== null) {
    const value = Number.parseFloat(relative[1])
    if (Number.isFinite(value) && value > 0) return scale(value, relative[2])
  }

  return undefined
}

/** 把「数值 + 单位」换成毫秒。没写单位按秒算 —— 限流说的都是秒。 */
function scale(value, unit) {
  const text = typeof unit === 'string' ? unit.toLowerCase() : ''
  if (text.startsWith('ms') || text.startsWith('milli')) return value
  if (text.startsWith('m') && !text.startsWith('ms')) return value * 60_000
  return value * 1000
}

/**
 * 这次限流该让这个模型歇多久。
 *
 * 优先级：上游明说的 > 设置里的默认值。上游说 5 秒就歇 5 秒 —— 它比我们更
 * 清楚自己什么时候能接活。
 *
 * @param {{code?: string, message?: string, retryAfterMs?: number}} failure
 * @param {number} now
 * @param {number} fallbackMs - 设置里的默认冷却时长
 * @param {number} maxMs - 冷却时长的硬上界
 * @returns {number}
 */
export function cooldownMsFor(failure, now, fallbackMs, maxMs) {
  const hinted = failure?.retryAfterMs
  let ms
  if (Number.isFinite(hinted) && hinted > 0) ms = hinted
  else {
    const parsed = retryAfterFromMessage(failure?.message, now)
    ms = parsed === undefined ? fallbackMs : parsed
  }
  if (!Number.isFinite(ms) || ms <= 0) ms = fallbackMs
  const ceiling = Number.isFinite(maxMs) && maxMs > 0 ? maxMs : 60 * 60 * 1000
  // 下界一秒：上游说 200ms 的时候按 200ms 排一次没意义，队列还没走到它就到了。
  return Math.min(ceiling, Math.max(1000, Math.round(ms)))
}

/**
 * 冷却表 —— 纯内存，只活这一程。
 *
 * 键是 `routeKey(provider, model)`，值是「到这一刻为止别用它」。**不分会话、
 * 不分模板**：限流是按供应商的账号算的，甲会话撞了限流，乙会话再打过去只会
 * 让它更慢 —— 这一点和隔离（按会话 × 模板）刚好相反，别把两者混成一件事。
 *
 * 过期的条目在读的时候顺手丢掉，不需要定时器。
 */
export class Cooldowns {
  /** @param {{ now?: () => number }} [options] */
  constructor(options = {}) {
    this.now = typeof options.now === 'function' ? options.now : () => Date.now()
    /** @type {Map<string, {until: number, code: string, message: string, at: number, provider: string, model: string}>} */
    this.entries = new Map()
    /** 已经开过几趟调用。见 `beginCall()`。 */
    this.calls = 0
    /** 此刻还**没跑完**的调用:趟号 → 它的开工时刻。 */
    this.open = new Map()
  }

  /**
   * 一趟模型调用开工了。
   *
   * 为什么要记这个：冷却到期不等于马上就能把队首抢回来。上游的限流是按它的
   * 账号算的，而**正在跑的那一趟**是别的模型在替它干活 —— 那一趟跑到一半就把
   * 队首还回去，等于用户手上正写着的半句话被从中间换个人接着写。所以规则是：
   * 冷却到期只说明「可以排队了」，真正回队首还要等那一趟跑完（或者它报错收场）。
   *
   * @returns {number} 这一趟的趟号，收工时交给 `endCall()`。
   */
  beginCall() {
    this.calls += 1
    this.open.set(this.calls, this.now())
    return this.calls
  }

  /** 这一趟跑完了（成功、报错、被取消都算）。 */
  endCall(call) {
    if (!Number.isInteger(call)) return false
    return this.open.delete(call)
  }

  /**
   * 冷却早就到点了，但因为「还有人在替它跑这一趟」，现在还不能回队首。
   *
   * 判据只有一条：有没有哪一趟调用是在它冷却到期**之前**就开工、到现在还没收工。
   * 有，就是还在被替着；没有，它就可以回去了。
   *
   * @param {string} key
   * @returns {boolean}
   */
  held(key) {
    const entry = this.entries.get(key)
    if (entry === undefined) return false
    if (entry.until > this.now()) return false
    for (const startedAt of this.open.values()) {
      if (startedAt < entry.until) return true
    }
    return false
  }

  /** 这个模型此刻还在冷却里吗（「等这一趟跑完」的也算）。 */
  has(key) {
    const entry = this.entries.get(key)
    if (entry === undefined) return false
    if (entry.until > this.now()) return true
    if (this.held(key)) return true
    // 到点了、也没人替它跑：顺手扔掉，免得表一直长。
    this.entries.delete(key)
    return false
  }

  /** 此刻所有还在冷却里的键（含等着的，顺手清掉真正过期的）。 */
  keys() {
    const at = this.now()
    const live = new Set()
    for (const [key, entry] of this.entries) {
      if (entry.until > at) live.add(key)
      else if (this.held(key)) live.add(key)
      else this.entries.delete(key)
    }
    return live
  }

  /** 还剩多少毫秒（已过期或没有就是 0）。 */
  remaining(key) {
    const entry = this.entries.get(key)
    if (entry === undefined) return 0
    return Math.max(0, entry.until - this.now())
  }

  /**
   * 让这个模型歇一会儿。
   *
   * @param {string} key
   * @param {{provider?: string, model?: string, code?: string, message?: string, ms: number, at?: number}} input
   * @returns {{until: number, ms: number}} 排到什么时候
   */
  put(key, input) {
    const at = Number.isFinite(input?.at) ? input.at : this.now()
    const ms = Number.isFinite(input?.ms) && input.ms > 0 ? Math.round(input.ms) : 0
    // 已经在冷却里且新排的结束得更早，就别把时间缩短 —— 上游第二次报的
    // 「再等 5 秒」通常是从**现在**起算的，但队列刚刚才因为第一次撞墙绕开它，
    // 缩短等于让它在还没缓过来的时候又被戳一下。
    const previous = this.entries.get(key)
    const until = previous !== undefined && previous.until > at + ms ? previous.until : at + ms
    this.entries.set(key, {
      provider: typeof input?.provider === 'string' ? input.provider : '',
      model: typeof input?.model === 'string' ? input.model : '',
      code: typeof input?.code === 'string' ? input.code : '',
      message: typeof input?.message === 'string' ? input.message.slice(0, 400) : '',
      at,
      until,
    })
    return { until, ms: until - at }
  }

  /** 立刻把某条冷却摘掉（它刚刚产出内容，说明早就缓过来了）。 */
  delete(key) {
    return this.entries.delete(key)
  }

  /** 清空（用户点「解除隔离」时顺手把冷却也解了 —— 那是同一个「重新试一次」的意图）。 */
  clear() {
    const count = this.entries.size
    this.entries.clear()
    return count
  }

  /** 给界面看的一份平铺列表（含还在冷却的、以及「等这一趟跑完」的）。 */
  view() {
    const at = this.now()
    const rows = []
    for (const [key, entry] of this.entries) {
      if (entry.until > at) {
        rows.push({ key, ...entry, held: false, remainingMs: entry.until - at })
        continue
      }
      // 到点了，但替它跑的那一趟还没收工 —— 留在表里，让界面说清楚它在等什么。
      if (this.held(key)) {
        rows.push({ key, ...entry, held: true, remainingMs: 0 })
        continue
      }
      this.entries.delete(key)
    }
    rows.sort((left, right) => left.until - right.until)
    return rows
  }
}
