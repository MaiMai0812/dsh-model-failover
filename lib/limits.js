/**
 * 每个模型自己的**并发上限** —— 「这个上游同时只收得下 N 个请求」。
 *
 * 为什么需要它：队列里有些模型是转发的多账号反代或小额度中转，上游明说了
 * 「同一时刻最多 N 个请求」，超了不是排队，是直接报错。接力插件本来会在
 * 一个模型失败时把**整个会话**推到下一个候选，所以只要有两个会话同时用上
 * 那个模型，第二个就会撞出一个跟模型质量毫无关系的错误，然后被隔离一整天
 * —— 用户看到的就是「这模型明明好好的，怎么今天一直坏」。
 *
 * 所以这里单开一本账：给候选设一个上限，**每有一个会话正在用它就占掉一格**。
 * 还有空位就照常允许别的会话进来；占满了就把这个候选从这一趟的队列里摘掉，
 * 让会话顺着队列往下走（见 `lib/adapter.js` 的 `chain()`）。别的会话释放之后
 * 它自动回到原位 —— 下一趟调用重新挑人时就会发现它又空了。
 *
 * 三条刻意的设计：
 *
 *  1. **上限是可选的。** 没设上限的候选完全不受影响，`usable()` 一律说能进。
 *     这是为了让这本账在没人配置的时候彻底隐形，不会悄悄改变已有行为。
 *  2. **按会话计数，不按请求计数。** 同一个会话同时发两个请求（主调用 + 压缩）
 *     只算占一格：上限说的是「几个会话在用它」，同一个会话自己跟自己抢没有意义。
 *  3. **用完要歇一会儿。** 最后一格还回去之后，这个候选还要**冷却**一小段
 *     （`DEFAULT_SETTINGS.limitCooldownSec`，默认 5 秒）才重新接客。理由是上游
 *     那边的连接/配额通常比本地慢半拍：本地一放手就立刻塞下一个请求进去，
 *     很容易撞出「你刚才不是还在跑吗」那种 429。冷却只跟着设了上限的候选走
 *     —— 没设上限的模型本来就不在这个问题的射程里。
 *  4. **只在内存里。** 上限本身跟着模板落盘（那是用户设的），但**占用**和冷却
 *     只活在内存里 —— 进程重启后没有任何请求在跑，把占用一起忘掉才是对的。
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
  /** @type {Map<string, Map<string, {at: number, count: number}>>} 候选 → 会话 → 占用情况 */
  #held = new Map()

  /** @type {Map<string, number>} 候选 → 冷却到什么时候（epoch ms） */
  #cooled = new Map()

  /** @type {() => number} */
  #now

  /** @type {() => number} */
  #cooldownMs

  /**
   * @param {{now?: () => number, cooldownMs?: () => number}} [options]
   *   `cooldownMs` 是个**函数**，因为用户在设置页改那个数字时不用重建这本账。
   */
  constructor({ now = Date.now, cooldownMs = () => 0 } = {}) {
    this.#now = now
    this.#cooldownMs = cooldownMs
  }

  /**
   * 这个候选现在还能不能接一个新会话。
   *
   * 判据就是用户描述的那一条：还有空位就允许，占满了就跳过。自己已经占着的
   * 时候永远允许 —— 那是同一趟调用在换账号重发，不该被自己的占用挡在门外。
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
    // 刚有人用完、还在歇着：这一趟先绕开它。
    if (this.coolingMs(key) > 0) return false
    if (holders === undefined) return true
    return holders.size < cap
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
    const until = this.#cooled.get(key)
    if (until === undefined) return 0
    const left = until - this.#now()
    if (left <= 0) {
      this.#cooled.delete(key)
      return 0
    }
    return left
  }

  /**
   * 记下「这个会话正在用这个候选」。
   *
   * @param {string} key
   * @param {number|undefined} cap 没设上限的候选根本不记账
   * @param {string|undefined} sessionId
   */
  acquire(key, cap, sessionId) {
    if (cap === undefined || sessionId === undefined) return false
    let holders = this.#held.get(key)
    if (holders === undefined) {
      holders = new Map()
      this.#held.set(key, holders)
    }
    const previous = holders.get(sessionId)
    holders.set(sessionId, { at: previous?.at ?? this.#now(), count: (previous?.count ?? 0) + 1 })
    return true
  }

  /**
   * 这个会话用完了，把那一格还回去。
   *
   * 计数归零才真的摘掉：同一个会话可能同时开着两个请求，先收工的那个不该
   * 替另一个把位置让出去。
   *
   * @param {string} key
   * @param {string|undefined} sessionId
   */
  release(key, cap, sessionId) {
    if (cap === undefined || sessionId === undefined) return false
    const holders = this.#held.get(key)
    if (holders === undefined) return false
    const previous = holders.get(sessionId)
    if (previous === undefined) return false
    if (previous.count > 1) {
      holders.set(sessionId, { at: previous.at, count: previous.count - 1 })
      return true
    }
    holders.delete(sessionId)
    if (holders.size === 0) {
      this.#held.delete(key)
      // 位置空出来了，但**先别急着让别人进来**：上游那边通常比本地慢半拍，
      // 本地一放手就立刻塞下一个请求，很容易撞出个跟模型质量毫无关系的 429。
      // 所以最后一格还回去的同时记一笔冷却，到点自动恢复。
      const ms = this.#cooldownMs()
      if (ms > 0) this.#cooled.set(key, this.#now() + ms)
    }
    return true
  }

  /** 这个会话是不是正占着这个候选。 */
  holds(key, sessionId) {
    return this.#held.get(key)?.has(sessionId) === true
  }

  /** 几个会话正占着（没设上限的候选也照实报，界面要显示）。 */
  used(key) {
    return this.#held.get(key)?.size ?? 0
  }

  /** 正占着的会话号，按开始时间排序。 */
  holders(key) {
    const holders = this.#held.get(key)
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
   * @returns {Array<{key: string, cap: number, used: number, holders: string[], provider: string, model: string}>}
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
    this.#cooled.clear()
    return count
  }
}
