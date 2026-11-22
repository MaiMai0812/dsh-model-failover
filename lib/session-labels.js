/**
 * 让**费用相关的读者**看到的模型名是真正跑过的那个，而不是本插件的路由名。
 *
 * 背景（用户原话：「调用模型显示的费用还是 model-failover，请修改」，附的是会话页
 * 「本轮费用」折叠卡：三条明细全写着 `model-failover:default · 输入/输出/缓存读取`）。
 *
 * ── 这条卡片的数据是从哪来的 ────────────────────────────────────────────────
 *
 * 它**不读账本**，而是 dsh-cost-meter 的 `getTurnCost` RPC：
 *
 *     turn-cost.js  getTurnCost → sessionUsageRecords(ctx, sessionId)
 *                   → sessions.get(sessionId).snapshotEvents()   ← 会话事件流
 *                   → usageRecord() 只保 provider/model 两个字段
 *                   → replaySessionRecords() → costFromSamples()
 *                   → rows: [{ provider: sample.provider, model: sample.model, … }]
 *
 * 而宿主写 `assistant/message` 时，`source.provider/model` 取的是**外层请求**的
 * 身份 —— 也就是本插件的路由名（`request` 由 `markAgentLoopRequest(Object.freeze(…))`
 * 冻结，适配器改不了）。真实模型名一直躺在同一条消息的
 * `source.replayState.response.{provider,model}` 里（实测 2626/2626 条消息的
 * `source.provider` 都是路由名，其中 2364 条带着真名），但 cost-meter 的
 * `usageRecord()` 明确只拷 provider/model 两个字段，replayState 被丢掉。
 *
 * ── 为什么不能改那条消息 ──────────────────────────────────────────────────
 *
 * `createMessage()` 里是 `deepFreeze(structuredClone({ … }))`，事件对象整棵冻死；
 * `snapshotEvents()` 返回的也是冻结数组。改不了，只能**在交给读者时换一份副本**。
 *
 * ── 换给谁：只换给「算钱的人」 ────────────────────────────────────────────
 *
 * `sessions.get(id).snapshotEvents()` 在宿主里有几十个读者，其中好几个是**写盘**
 * 的（会话持久化把没存过的尾巴读出来落盘）、做投影的、跟会话分叉的。把真名塞给
 * 它们等于改写宿主持久数据，绝不能干。所以这里只对 **dsh-cost-meter 的调用**换名：
 *
 * - `costMeter` 服务的每个方法包一层 `AsyncLocalStorage`（`audience`）标记；
 * - `sessions.get()` 返回的门面，只在标记存在时才换名，别的时候原样返回。
 *
 * 换名之后价格也跟着对：`replaySessionRecords()` 是**先取名字再取价**，名字换了，
 * 单价就是那个模型自己的价，不用另外改账。
 *
 * 已知边界：只对**热会话**有效（cost-meter 走 `session.snapshotEvents()` 那条）；
 * 冷会话它是直接读 `$DSH_HOME/sessions` 下的 JSONL，那条路上我们插不进去 ——
 * 历史会话里仍旧是路由名。这是有意的取舍：宁可少改一处，也不去改宿主的磁盘格式。
 *
 * 开关沿用设置页的「按真正跑的那个模型记账」(`billByRealModel`)：账单、费用卡、
 * 会话费用页本来就该是同一件事，分两个开关只会让人配出一半。
 */

import { AsyncLocalStorage } from 'node:async_hooks'

/**
 * 记在服务对象上：装过我们这层没有（插件热重载会再来一次 apply）。
 * 用 Symbol 而不是字符串字段，免得和宿主的属性撞名。
 */
const WRAPPED = Symbol.for('model-failover.session-labels')
const METER_WRAPPED = Symbol.for('model-failover.session-labels.meter')

/** 「此刻正在替 cost-meter 读会话」—— 只有它在的时候才换名。 */
const audience = new AsyncLocalStorage()

/** 随时可读的配置：路由名哪来的、用户还开着吗。 */
let config = { route: '', enabled: () => true }

/** 装之前服务上有没有自己的 `get`；卸载时照原样还原。 */
let restore = undefined

/** 已经装到哪个服务对象上了 —— 用来判断「换了个服务对象」要重装。 */
let patched = undefined

/** costMeter 那张表：方法名 → 原函数的描述符，卸载时逐条还原。 */
let metered = undefined

/** 会话对象 → 包装后的门面。同一会话反复 `get()` 拿到的是同一个壳（省得每次新建）。 */
const faces = new WeakMap()

/** 状态：给 `/state` 与设置页看，故障不该只藏在账本里。 */
const stats = { installedAt: 0, relabeled: 0, failures: 0, reason: '', audience: '' }

/**
 * 读一条事件里的真名；不是本路由、没有 replay 真名、名字不合法，都返回 undefined。
 *
 * 只认 `assistant/message`：`assistant/attempt`（失败重试留下的痕迹）没有正文，
 * 换算过去只会把「哪次尝试」也讲成「哪个模型答的」，反而误导。
 *
 * @param {object} event
 * @param {string} route
 * @returns {{provider: string, model: string} | undefined}
 */
function realOf(event, route) {
  if (event?.type !== 'assistant/message') return undefined
  const source = event.data?.message?.source
  if (source === null || typeof source !== 'object') return undefined
  // 只换本路由写下的那一条；别的插件/原生 provider 的消息原样放回去。
  if (source.provider !== route) return undefined
  // `kind` 不是 model 的（用户、开发者消息）不该走到这儿，兜一下。
  if (source.kind !== undefined && source.kind !== 'model') return undefined
  const response = source.replayState?.response
  if (response === null || typeof response !== 'object') return undefined
  const provider = typeof response.provider === 'string' ? response.provider.trim() : ''
  const model = typeof response.model === 'string' ? response.model.trim() : ''
  if (provider === '' || model === '' || provider === source.provider) return undefined
  return { provider, model }
}

/**
 * 把一批事件里属于本路由的那些换成真名。**没得换就返回原数组本身**（同一个引用），
 * 这样绝大多数调用（没有模型的会话、非本路由的消息）连一次拷贝都不做。
 *
 * @param {readonly object[]} events
 * @param {string} route
 * @returns {readonly object[]}
 */
export function relabelEvents(events, route) {
  if (!Array.isArray(events) || events.length === 0 || typeof route !== 'string' || route === '') return events
  let touched = false
  let count = 0
  const out = events.map((event) => {
    const real = realOf(event, route)
    if (real === undefined) return event
    touched = true
    count += 1
    const data = { ...event.data }
    const message = { ...data.message }
    message.source = { ...message.source, provider: real.provider, model: real.model }
    data.message = message
    return { ...event, data }
  })
  if (!touched) return events
  stats.relabeled += count
  return out
}

/**
 * 给一个会话对象套上「读出来是真名」的壳。
 *
 * 用 Proxy 而不是手写门面：会话对象上的东西远不止 `snapshotEvents`（`seq`、`header`、
 * `eventAt`、`firstLiveSeq`…），漏一个就可能让读者拿到 undefined。
 *
 * 两个必须注意的点：
 * - 取值时 `Reflect.get(target, key, target)` 把 `this` 钉在原对象上 —— 这些类用的是
 *   私有字段（`#log`），`this` 一旦是 Proxy 就会直接 `TypeError`；
 * - 方法要 `bind(target)` 后再给出去，同样是为了私有字段，而且这样 `wrapper.seq`
 *   这类取值器也能拿到正确的值。
 *
 * @param {object} face
 * @param {string} route
 * @returns {object}
 */
function wrapFace(face, route) {
  if (face === null || typeof face !== 'object') return face
  const cached = faces.get(face)
  if (cached !== undefined) return cached
  const bound = new Map()
  const wrapper = new Proxy(face, {
    get(target, key) {
      if (key === 'snapshotEvents') {
        return (...args) => relabelEvents(target.snapshotEvents(...args), route)
      }
      if (key === 'ownEvents') {
        return (...args) => relabelEvents(target.ownEvents(...args), route)
      }
      if (key === 'eventAt') {
        return (seq) => relabelEvents([target.eventAt(seq)], route)[0]
      }
      const value = Reflect.get(target, key, target)
      if (typeof value !== 'function') return value
      let fn = bound.get(key)
      if (fn === undefined) {
        fn = value.bind(target)
        bound.set(key, fn)
      }
      return fn
    },
  })
  faces.set(face, wrapper)
  return wrapper
}

/** 现在该不该换名：开关关掉就不换；没有 cost-meter 在读就也不换。 */
function live() {
  try {
    if (config.enabled() === false) return false
  } catch {
    /* 读不到设置就当开着 —— 最坏也就是多换一次名字，而不会算错钱 */
  }
  return audience.getStore() === true
}

/**
 * 包一层 `sessions` 服务的 `get`，只在「算钱的人」来读时换名。
 *
 * @param {object} service - `ctx.get('sessions')` 拿到的那个服务（会话存储）
 * @param {{route: string, enabled?: () => boolean}} options
 * @returns {{ok: boolean, reason: string}}
 */
export function installSessionLabels(service, options = {}) {
  if (typeof options.route === 'string' && options.route !== '') config.route = options.route
  if (typeof options.enabled === 'function') config.enabled = options.enabled
  if (service === null || typeof service !== 'object') {
    stats.reason = 'no-service'
    return { ok: false, reason: 'no-service' }
  }
  if (service[WRAPPED] === true && patched === service) {
    stats.reason = 'already'
    return { ok: true, reason: 'already' }
  }
  const original = service.get
  if (typeof original !== 'function') {
    stats.reason = 'no-get'
    return { ok: false, reason: 'no-get' }
  }
  try {
    const own = Object.getOwnPropertyDescriptor(service, 'get')
    service.get = function get(sessionId) {
      const face = original.call(this, sessionId)
      // 不是算钱的路过的，原样给 —— 宿主自己写盘、分叉、做投影都走这一支。
      if (!live()) return face
      return wrapFace(face, config.route)
    }
    try {
      Object.defineProperty(service, WRAPPED, { value: true, configurable: true, enumerable: false })
    } catch {
      /* 服务对象不许加字段也不要紧：`patched` 已经记下来了 */
    }
    patched = service
    restore = { service, own }
    stats.installedAt = Date.now()
    stats.reason = 'installed'
    return { ok: true, reason: 'installed' }
  } catch (error) {
    stats.reason = 'patch-failed'
    stats.failures += 1
    return { ok: false, reason: 'patch-failed', message: String(error?.message ?? error) }
  }
}

/** 收集一个对象身上所有函数方法名（含原型链，停在 Object.prototype 之前）。 */
function methodNames(service) {
  const names = new Set()
  let cursor = service
  while (cursor !== null && cursor !== undefined && cursor !== Object.prototype) {
    for (const key of Object.getOwnPropertyNames(cursor)) {
      if (key === 'constructor' || names.has(key)) continue
      const descriptor = Object.getOwnPropertyDescriptor(cursor, key)
      if (typeof descriptor?.value === 'function') names.add(key)
      else if (typeof descriptor?.get === 'function') {
        try {
          if (typeof descriptor.get.call(service) === 'function') names.add(key)
        } catch {
          /* 取值器炸了就跳过 */
        }
      }
    }
    cursor = Object.getPrototypeOf(cursor)
  }
  return [...names]
}

/**
 * 包一层 `costMeter` 服务的每个方法：调它们的时候挂上「正在算钱」的标记。
 *
 * 为什么是标记而不是直接改返回值：费用卡的行名和单价是同一次回放算出来的
 * （先取名字、再取这个模型自己的价），在事件那一层换名，价格就自动跟着对；
 * 事后改 `rows[].provider` 只会把名字换掉、单价还是路由名那一档的。
 *
 * @param {object} service - `ctx.get('costMeter')`
 * @returns {{ok: boolean, reason: string, methods?: number}}
 */
export function installCostReader(service, options = {}) {
  if (typeof options.route === 'string' && options.route !== '') config.route = options.route
  if (typeof options.enabled === 'function') config.enabled = options.enabled
  if (service === null || typeof service !== 'object') {
    stats.audience = 'no-cost-meter'
    return { ok: false, reason: 'no-cost-meter' }
  }
  if (service[METER_WRAPPED] === true && metered?.service === service) {
    stats.audience = 'already'
    return { ok: true, reason: 'already' }
  }
  // 换了个服务对象（宿主重发服务、或测试里连着装两处）：先把上一份还原，
  // 否则卸载只认最后一个，先前那个服务上的壳就永远摘不掉了。
  if (metered !== undefined && metered.service !== service) {
    const { service: previous, table } = metered
    for (const [name, { original, had }] of table) {
      try {
        if (had === undefined) delete previous[name]
        else Object.defineProperty(previous, name, had)
      } catch {
        /* 尽力而为 */
      }
    }
    metered = undefined
  }
  const names = methodNames(service)
  if (names.length === 0) {
    stats.audience = 'no-methods'
    return { ok: false, reason: 'no-methods' }
  }
  const table = new Map()
  try {
    for (const name of names) {
      const original = service[name]
      if (typeof original !== 'function') continue
      const had = Object.getOwnPropertyDescriptor(service, name)
      table.set(name, { original, had })
      service[name] = function metered(...args) {
        // `run` 把标记沿整条异步链带下去：cost-meter 内部 await 多少次都还在。
        return audience.run(true, () => original.apply(this, args))
      }
    }
    try {
      Object.defineProperty(service, METER_WRAPPED, { value: true, configurable: true, enumerable: false })
    } catch {
      /* 同上，`metered` 已经记下来了 */
    }
    metered = { service, table }
    stats.audience = 'installed'
    return { ok: true, reason: 'installed', methods: table.size }
  } catch (error) {
    // 半装不装比不装更糟：失败就当场回滚已经换掉的那几个。
    for (const [name, { original, had }] of table) {
      try {
        if (had === undefined) delete service[name]
        else Object.defineProperty(service, name, had)
      } catch {
        /* 尽力而为 */
      }
    }
    stats.audience = 'patch-failed'
    stats.failures += 1
    return { ok: false, reason: 'patch-failed', message: String(error?.message ?? error) }
  }
}

/**
 * 把两张表都还原回去（插件卸载时调用）。
 *
 * @returns {boolean}
 */
export function releaseSessionLabels() {
  let released = false
  if (metered !== undefined) {
    const { service, table } = metered
    for (const [name, { original, had }] of table) {
      try {
        if (had === undefined) delete service[name]
        else Object.defineProperty(service, name, had)
        released = true
      } catch {
        /* 尽力而为 */
      }
    }
    try {
      delete service[METER_WRAPPED]
    } catch {
      /* 不可配置也无所谓：`metered` 已经清了 */
    }
    metered = undefined
  }
  if (patched !== undefined) {
    const { service, own } = restore ?? {}
    try {
      if (service !== undefined) {
        if (own !== undefined && 'value' in own) Object.defineProperty(service, 'get', own)
        else delete service.get
        try {
          delete service[WRAPPED]
        } catch {
          /* 加了不可配置的属性也没关系，下次 install 会看 `patched` */
        }
        released = true
      }
    } catch {
      released = false
    }
    patched = undefined
    restore = undefined
  }
  return released
}

/** 这一块装上了没有、换过多少条消息 —— 给 `/state` 用。 */
export function sessionLabelStatus() {
  let attached = false
  try {
    attached = patched !== undefined && patched[WRAPPED] === true && metered !== undefined
  } catch {
    attached = false
  }
  return {
    attached,
    installedAt: stats.installedAt,
    relabeled: stats.relabeled,
    failures: stats.failures,
    reason: stats.reason,
    audience: stats.audience,
  }
}

/** 测试用：把计数清零，免得用例之间互相看见对方的数字。 */
export function resetSessionLabelStats() {
  stats.installedAt = 0
  stats.relabeled = 0
  stats.failures = 0
  stats.reason = ''
  stats.audience = ''
}