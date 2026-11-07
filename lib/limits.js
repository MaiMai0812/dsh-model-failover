/**
 * 每个模型自己的**并发上限** —— 「这个上游同时只收得下 N 个请求」。
 *
 * 为什么需要它：队列里有些模型是转发的多账号反代或小额度中转，上游明说了
 * 「同一时刻最多 N 个请求」，超了不是排队，是直接报错。接力插件本来会在
 * 一个模型失败时把**整个会话**推到下一个候选，所以只要有两个会话同时用上
 * 那个模型，第二个就会撞出一个跟模型质量毫无关系的错误，然后被隔离一整天
 * —— 用户看到的就是「这模型明明好好的，怎么今天一直坏」。
 *
 * 所以这里单开一本账：给候选设一个上限，**一个会话占一格**。
 * 还有空位就照常允许别的会话进来；占满了就把这个候选从这一趟的队列里摘掉，
 * 让会话顺着队列往下走（见 `lib/adapter.js` 的 `chain()`）。名额交还之后
 * 它自动回到原位 —— 下一趟调用重新挑人时就会发现它又空了。
 *
 * 五条刻意的设计：
 *
 *  1. **座位是会话级的租约，不是请求级的。** 会话一旦抢到一个座位就**钉在那儿**
 *     （`hold()`），这一轮里换账号重发、补「继续」、写完整轮都不放手；只有
 *     「这个模型报错了」（`free()`）或「会话结束了」（`freeSession()`）才交还。
 *     早先按请求记账（每发一次请求 acquire、收工 release）会带来一个很难看的
 *     现象：一轮对话里每跳之间都把座位让出去、还顺手触发收工冷却，于是会话
 *     被自己的账推着「时不时放弃这个模型去用下一个」，正是用户报的那个问题。
 *  2. **上限是可选的。** 没设上限的候选完全不受影响，`usable()` 一律说能进。
 *     这是为了让这本账在没人配置的时候彻底隐形，不会悄悄改变已有行为。
 *  3. **用完要歇一会儿。** 座位上最后一个人交还之后，这个候选还要**冷却**一小段
 *     （`DEFAULT_SETTINGS.limitCooldownSec`，默认 5 秒）才重新接客。理由是上游
 *     那边的连接/配额通常比本地慢半拍：本地一放手就立刻塞下一个请求进去，
 *     很容易撞出「你刚才不是还在跑吗」那种 429。冷却只跟着设了上限的候选走
 *     —— 没设上限的模型本来就不在这个问题的射程里。**刚交还的那个会话本人
 *     回来时不歇**，否则用户连发两句就会被换到下一个模型上，又绕回问题 1。
 *  4. **坐太久没人碰的座位会被收回。** 会话开着、却再也不碰这个模型也是常态；
 *     没有这条，那一格会被一个不会回来的会话永久占住。判据是「满了、又有人
 *     要用」，此时把超过 `IDLE_MS` 没刷新过的座位收掉 —— 不做后台定时清扫。
 *  5. **只在内存里。** 上限本身跟着模板落盘（那是用户设的），但**座位**和冷却
 *     只活在内存里 —— 进程重启后没有任何请求在跑，把座位一起忘掉才是对的。
 */

/** 上限最多能设到多少。设成 16 是因为再多就不是「这个上游收不下」了。 */
export const MAX_CONCURRENCY = 16

/**
 * 从一个候选条目上读「并发上限」。
 *
 * 只有正整数才算数；`0`、负数、小数、字符串一律当作「没设」—— 尤其是 `0`：
 * 把 `0` 理解成「谁都不许用」会让那个候选永远躺在队列里占着位置，
 * 而用户想表达的显然是「我不限制」。
 *
 * @param {unknown} raw
 * @returns {number|undefined}
 */
export function concurrencyOf(raw) {
  if (!Number.isInteger(raw) || raw <= 0) return undefined
  return Math.min(raw, MAX_CONCURRENCY)
}

export class Concurrency {
  /** @type {Map<string, Map<string, {at: number}>>} 候选 → 会话 → 座位 */
  #held = new Map()

  /** @type {Map<string, string>} 会话 → 它钉住的候选（租约的索引） */
  #pinned = new Map()

  /**
   * 坐了这么久还没被碰过，就当它不要了 —— 把名额让给真在跑活的会话。
   *
   * 为什么需要：租约的目的是「一个会话一直用它」，但**会话一直开着、却再也不
   * 碰这个模型**也是常态（用户切走了、只发了别的消息、开着窗口去忙别的）。
   * 没有这条，那一格会被一个不会回来的会话永久占住，别人只看到「上限明明有
   * 空位、怎么一直轮不到」—— 那比早先的「时不时换模型」更难查。
   *
   * 只在**别人要用、但没空位**的时候才顺手收（见 `usable()`），不做定时清扫：
   * 账本里留着一条没人问的旧座位无害，凭空多一个后台定时器反而是负担。
   */
  static IDLE_MS = 30 * 60 * 1000

  /**
   * @type {Map<string, {until: number, by?: string}>} 候选 → 冷却到什么时候（epoch ms），
   * 以及「是谁刚收工触发的这一歇」。
   *
   * `by` 只有一个用处：那个会话自己回来时**不歇**。否则会出现一个很别扭的循环 ——
   * 会话这一轮用完、座位一还就进了 5 秒冷却，用户再发一句，它就因为「正在歇」
   * 被换到下一个模型上；用户看到的是「这插件怎么老自己换模型」。冷却的本意是
   * 别让**另一个**会话紧贴着上一个会话撞上去，不是挡住刚干完活的那个会话本人。
   */
  #cooled = new Map()

  /** @type {() => number} */
  #now

  /** @type {() => number} */
  #cooldownMs

  /** @type {number} */
  #idleMs

  /**
   * @param {{now?: () => number, cooldownMs?: () => number, idleMs?: number}} [options]
   *   `cooldownMs` 是个**函数**，因为用户在设置页改那个数字时不用重建这本账。
   */
  constructor({ now = Date.now, cooldownMs = () => 0, idleMs = Concurrency.IDLE_MS } = {}) {
    this.#now = now
    this.#cooldownMs = cooldownMs
    this.#idleMs = Number.isFinite(idleMs) && idleMs > 0 ? idleMs : Concurrency.IDLE_MS
  }

  /**
   * 这个候选现在还能不能接一个新会话。
   *
   * 判据就是用户描述的那一条：还有空位就允许，占满了就跳过。自己已经占着的
   * 时候永远允许 —— 那是同一趟调用在换账号重发，不该被自己的座位挡在门外。
   *
   * @param {string} key
   * @param {number|undefined} cap 没设上限时一律放行
   * @param {string|undefined} sessionId 没有会话号（压缩、离线调用）时按「新会话」算
   * @returns {boolean}
   */
  usable(key, cap, sessionId) {
    if (cap === undefined) return true
    const holders = this.#held.get(key)
    if (sessionId !== undefined && holders?.has(sessionId) === true) return true
    // 刚有人用完、还在歇着：这一趟先绕开它 —— 但**刚收工的那个会话本人除外**，
    // 否则用户连发两句就会被换到下一个模型上（见 `#cooled` 的注释）。
    const cooled = this.#cooled.get(key)
    if (cooled !== undefined) {
      const left = cooled.until - this.#now()
      if (left <= 0) this.#cooled.delete(key)
      else if (sessionId === undefined || cooled.by !== sessionId) return false
    }
    if (holders === undefined) return true
    if (holders.size < cap) return true
    // 满了 —— 但座位里可能有早就没人碰的（会话还开着、却再也没用过这个模型）。
    // 那几格本来就不该继续占着，顺手收回来再判一次。
    if (!this.#evictIdle(key, holders)) return false
    return holders.size < cap
  }

  /**
   * 把坐得太久的座位收回去。返回「有没有真的收掉」。
   *
   * 只在有人问「还有空位吗」的时候才走这条路：账本里留一条没人问的旧座位无害，
   * 凭空多一个后台定时器反而是负担。
   *
   * @param {string} key
   * @param {Map<string, {at: number}>} holders
   * @returns {boolean}
   */
  #evictIdle(key, holders) {
    const now = this.#now()
    let evicted = false
    for (const [sessionId, seat] of [...holders]) {
      if (now - seat.at < this.#idleMs) continue
      holders.delete(sessionId)
      if (this.#pinned.get(sessionId) === key) this.#pinned.delete(sessionId)
      evicted = true
    }
    if (holders.size === 0 && evicted) this.#held.delete(key)
    return evicted
  }

  /**
   * 拿这本账之前先扫一遍闲置的座位，免得界面把早就不在了的会话算成「正在用」。
   *
   * @param {string} key
   * @returns {Map<string, {at: number}>|undefined}
   */
  #seats(key) {
    const holders = this.#held.get(key)
    if (holders !== undefined) this.#evictIdle(key, holders)
    return this.#held.get(key)
  }

  /**
   * 这个候选还要歇多久（毫秒，0 = 没在歇）。
   *
   * 读的时候就顺手清掉过期的，省得那本账只增不减。
   *
   * @param {string} key
   * @returns {number}
   */
  coolingMs(key) {
    const cooled = this.#cooled.get(key)
    if (cooled === undefined) return 0
    const left = cooled.until - this.#now()
    if (left <= 0) {
      this.#cooled.delete(key)
      return 0
    }
    return left
  }

  /**
   * 这个会话手上的租约钉在哪个候选；没租约就是 `undefined`。
   *
   * 座位已经不在了（被 `clear()` 清掉、或者条目本身被删）时顺手把索引也清掉，
   * 免得留一条指向虚空的租约，让会话以为它还占着。
   *
   * @param {string|undefined} sessionId
   * @returns {string|undefined}
   */
  pinnedKey(sessionId) {
    if (typeof sessionId !== 'string' || sessionId === '') return undefined
    const key = this.#pinned.get(sessionId)
    if (key === undefined) return undefined
    if (this.#held.get(key)?.has(sessionId) !== true) {
      this.#pinned.delete(sessionId)
      return undefined
    }
    return key
  }

  /**
   * 把这个会话钉在这个候选上 —— 这就是「占一个并发名额」。
   *
   * 幂等：已经钉在同一个候选上时原样返回，不会把座位算成两格。钉到别处之前
   * 先把旧座位还掉（不留双份）；旧座位还的时候**不**触发收工冷却，因为那只是
   * 换了个候选，不是「跑了活刚收工」。
   *
   * @param {string} key
   * @param {number|undefined} cap 没设上限的候选根本不记账
   * @param {string|undefined} sessionId
   * @returns {boolean} 这次调用结束时会话是不是钉在这个候选上
   */
  hold(key, cap, sessionId) {
    if (cap === undefined || typeof sessionId !== 'string' || sessionId === '') return false
    const pinned = this.pinnedKey(sessionId)
    if (pinned !== undefined && pinned !== key) this.free(pinned, sessionId, { cool: false })
    let holders = this.#held.get(key)
    if (holders === undefined) {
      holders = new Map()
      this.#held.set(key, holders)
    }
    const seat = holders.get(sessionId)
    // 每次真跑请求都会经过这里，所以顺手把时间戳刷新一下 —— 它既是「什么时候
    // 坐下的」，也是「最近一次真的用到它是什么时候」（闲置收回要靠这个判据）。
    // 已经在同一格上（`pinned === key`）也要刷新，否则一个一直在用的会话会被
    // 当成闲置的收掉。
    if (seat === undefined) holders.set(sessionId, { at: this.#now() })
    else seat.at = this.#now()
    this.#pinned.set(sessionId, key)
    return true
  }

  /**
   * 交还这个会话手上的租约。
   *
   * 三种时机，调用方自己选：模型报错了、会话结束了、或者它被移出了模板。
   * 座位空出来时默认记一笔交还冷却 —— 但「报错」和「被移出模板」这两条
   * 不该让下一个会话再白等几秒（限流冷却表或隔离名单已经在拦着它了），
   * 它们传 `cool: false`。
   *
   * @param {string} key
   * @param {string|undefined} sessionId
   * @param {{cool?: boolean}} [options]
   * @returns {boolean} 这次调用有没有真的还掉一格
   */
  free(key, sessionId, { cool = true } = {}) {
    if (typeof sessionId !== 'string' || sessionId === '') return false
    if (this.#pinned.get(sessionId) === key) this.#pinned.delete(sessionId)
    const holders = this.#held.get(key)
    if (holders === undefined) return false
    if (!holders.delete(sessionId)) return false
    if (holders.size === 0) {
      this.#held.delete(key)
      if (cool === false) return true
      // 座位空出来了，但**先别急着让别人进来**：上游那边通常比本地慢半拍，
      // 本地一放手就立刻塞下一个请求，很容易撞出个跟模型质量毫无关系的 429。
      // 所以最后一格还回去的同时记一笔冷却，到点自动恢复。
      //
      // 记下是谁走的：它自己再回来时不歇（`usable()` 里那条例外）。收工冷却
      // 要拦的是**另一个**会话，不是刚干完活的这个。
      const ms = this.#cooldownMs()
      if (ms > 0) this.#cooled.set(key, { until: this.#now() + ms, by: sessionId })
    }
    return true
  }

  /**
   * 会话结束了：把它在所有候选上占的座位一次还清。
   *
   * 会话窗一关，它「钉住某个模型」的意图就没了 —— 名额要立刻还回去，但**先歇
   * 一下再让别人进来**（`cool` 默认 `true`，跟 `free()` 一致）：上游那边的配额
   * 核算比本地慢半拍，这个会话刚刚还在往上打请求，别人 0.1 秒后顶进去很容易撞出
   * 一个跟模型质量无关的 429。设置成 0 就是不歇。
   *
   * @param {string|undefined} sessionId
   * @param {{cool?: boolean}} [options]
   * @returns {number} 还掉了几格
   */
  freeSession(sessionId, { cool = true } = {}) {
    if (typeof sessionId !== 'string' || sessionId === '') return 0
    this.#pinned.delete(sessionId)
    let freed = 0
    for (const [key, holders] of [...this.#held]) {
      if (!holders.delete(sessionId)) continue
      freed += 1
      if (holders.size === 0) {
        this.#held.delete(key)
        if (cool === true) {
          const ms = this.#cooldownMs()
          if (ms > 0) this.#cooled.set(key, { until: this.#now() + ms, by: sessionId })
        }
      }
    }
    return freed
  }

  /** 这个会话是不是正占着这个候选。 */
  holds(key, sessionId) {
    return this.#seats(key)?.has(sessionId) === true
  }

  /** 几个会话正占着（没设上限的候选也照实报，界面要显示）。 */
  used(key) {
    return this.#seats(key)?.size ?? 0
  }

  /** 正占着的会话号，按开始时间排序。 */
  holders(key) {
    const holders = this.#seats(key)
    if (holders === undefined) return []
    return [...holders.entries()].sort((left, right) => left[1].at - right[1].at).map(([id]) => id)
  }

  /**
   * 给界面看的平铺列表。
   *
   * 传进来的 `caps` 是「当前关心的那些设了上限的候选」（键 → 上限），这样界面
   * 既能看到正在被占用的，也能看到空着没人在用的 —— 后者同样是用户想知道的信息
   * （「我设的上限到底有没有生效」）。
   *
   * @param {Map<string, {cap: number, provider?: string, model?: string}>} caps
   * @returns {Array<{key: string, cap: number, used: number, holders: string[], coolingMs: number, provider: string, model: string}>}
   */
  view(caps) {
    const rows = []
    if (!(caps instanceof Map)) return rows
    for (const [key, meta] of caps) {
      const cap = concurrencyOf(meta?.cap)
      if (cap === undefined) continue
      rows.push({
        key,
        cap,
        used: this.used(key),
        holders: this.holders(key),
        coolingMs: this.coolingMs(key),
        provider: typeof meta?.provider === 'string' ? meta.provider : '',
        model: typeof meta?.model === 'string' ? meta.model : '',
      })
    }
    rows.sort((left, right) => right.used - left.used || left.key.localeCompare(right.key))
    return rows
  }

  /** 清空（用户点「解除隔离」时一并清掉：那是同一个「重新开始」的意图）。 */
  clear() {
    const count = this.#held.size
    this.#held.clear()
    this.#pinned.clear()
    this.#cooled.clear()
    return count
  }
}