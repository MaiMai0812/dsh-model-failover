/**
 * 让 dsh-cost-meter 按「队列里真正跑的那个模型」记账。
 *
 * 背景（用户原话：「一轮会话后的计费还是按 model_failover 的名字来计费的，
 * 而不是实际调用的模型」）：
 *
 * 本插件是**包装路由** —— 宿主的 agent loop 发起的调用，`provider/model` 恒为
 * `model-failover/<模板 id>`，适配器再在自己的 `stream()` 体内换成本轮的真实
 * 候选重发一次。dsh-cost-meter 的实时计费钩子（`billing-stream.js`）在每次
 * `llm/stream` 上建一个计费 scope，收尾时：
 *
 *     const covered = proxy ? await proxy.finish(usage) : false
 *     if (usage !== null && !covered) account(usage, options?.model, options?.sessionId, …)
 *
 * `options` 是**外层**那一次调用的参数，于是记进账本的名字永远是路由名。走本机
 * `/bili/` 桥的那些调用能拿到代理收据（`covered === true`）而记账在代理那一侧，
 * 所以只有「直连上游」的候选会落进这句兜底 —— 正是用户看到的那一行。
 *
 * 外层身份改不了（宿主 `markAgentLoopRequest(Object.freeze({…}))` 冻结了它），
 * cost-meter 也没有登记别名/代记账的接口。能动的只有两处，都在本插件这一侧：
 *
 * 1. **让外层变透明**：桥对象挂在 `globalThis[Symbol.for(…)]` 上
 *    （`billion-context-billing.js` 写入），把它的 `createScope` 包一层：外层
 *    是这条路由时返回一个空壳，`finish()` 直接说「已覆盖」，那句兜底记账整句
 *    跳过，也不再为路由名写一份流元数据。
 * 2. **让内层被当成一次独立调用**：cost-meter 用 `AsyncLocalStorage` 标记「这条
 *    流的下游再发起的调用属于嵌套，不另记账」。适配器发起真实调用时改用
 *    **模块加载时**创建的 `AsyncResource` 把这一下放进干净上下文，内层于是
 *    以真实 `provider/model` 建自己的 scope，按真名与真价入账。
 *
 * 两件事必须成对：只让外层透明而不让内层落账 = 整单漏计；只让内层落账而不让
 * 外层透明 = 一笔调用记两次（`usageDeduper` 按 (model, 五桶) 配对，路由名和
 * 真名配不上，去重去不掉）。
 *
 * 所以 `finish()` 的回话是**有条件的**：只有本轮真的见过一个「真实模型」的
 * 计费 scope，才敢说「已覆盖」；否则老实返回 false，退回原来的兜底记账 ——
 * 宁可记错名字，也不能漏记或双记。整层都是 try/catch，cost-meter 改版、桥没装、
 * `createScope` 形状变了，都会安静地退回原行为。
 *
 * ── 为什么要「一直盯着」，而不是装一次就完事 ────────────────────────────────
 *
 * 第一版只在 `apply()` 之后装一次、失败了重试七八次就收手。运行中的宿主仍然把
 * 整天的调用记在路由名上，而探针（真的 `installBillionContextBilling` + 真的
 * `createLlmStreamBilling` + 真的本模块）却是绿的 —— 说明算法没错，是**补丁在
 * 那个进程里没有生效**。桥对象有几个可以被拿走的时机：
 *
 * - cost-meter 卸载/重载时 `dispose()` 会 `delete globalThis[桥]`，再装时是一份
 *   全新的 `api`，上面没有我们的标记；
 * - 本插件自己卸载时 `releaseBillingAttribution()` 会把原函数放回去，若同一
 *   fiber 之后又被挂起来（热重载），重试循环早已跑完，不会有人再装；
 * - 别的插件/宿主重启时序也可能让「装的那一刻桥还没来」。
 *
 * 所以现在改成三重保险：
 *
 * 1. 装的时候把 `globalThis[桥]` 定义成一个**访问器**：以后任何一次给这个 Symbol
 *    赋值，都会顺手把新桥包好（cost-meter 的 `dispose()` 用 `delete`，访问器是
 *    configurable 的，删得掉；再赋值时又会被包上）。
 * 2. `dispatchNested()` 每次派发前顺手看一眼：桥在、标记不在，就当场补装（限频）。
 * 3. `billingAttributionStatus()` 把「外层见过几次、真名落账几次」记下来，交给
 *    `/state` 与设置页显示 —— 这类故障以前是**完全不可见**的，只能靠翻账本猜。
 */

import { AsyncLocalStorage, AsyncResource } from 'node:async_hooks'

/** cost-meter 把桥对象挂在这个 Symbol 上（`billion-context-billing.js` 的 `bridgeKey`）。 */
const BRIDGE_KEY = Symbol.for('dsh-cost-meter.billion-context.v1')

/** 记在桥对象上：已经包过就不再包第二层（插件热重载会再来一遍 apply）。 */
const WRAPPED = Symbol.for('model-failover.billing-attribution')

/**
 * 「这一趟外层调用」的凭据，靠 ALS 从外层传到内层。
 *
 * 为什么要传：外层空壳的 `finish()` 得知道**这一轮**有没有落下真实模型的那一笔。
 * 用全局计数器判断会把并发的另一个会话算进来（DSH 常常同时跑好几个会话），
 * 那就会在一个会话逃逸失败时误判成「已覆盖」而漏记。
 */
const link = new AsyncLocalStorage()

/**
 * 模块加载时创建 —— 此刻没有正在跑的 `llm/stream` 监听器，所以它的异步上下文
 * 是「干净」的。用它发起真实调用，cost-meter 就看不出这其实是一次嵌套调用。
 *
 * 只在**同步派发**这一下生效：监听器是同步返回迭代器的，之后的 `next()` 仍走
 * 调用方自己的上下文，别的插件（以及宿主的 initiator 归因）一个字节都不受影响。
 */
const clean = new AsyncResource('model-failover:model-dispatch')

/**
 * 记在桥对象上：原来的 `createScope`。卸载时照着它还原。
 *
 * 不在这里放一个模块级的「装好了」布尔：桥对象可能被 cost-meter 换掉（重载、
 * 重新 apply），也可能在测试里每个用例各给一个替身。判断「包过没有」只认桥
 * 对象自己的标记，才是真的幂等。
 */
const RESTORE = Symbol.for('model-failover.billing-attribution.restore')

/**
 * 记在**内层请求参数**上：这一趟属于哪一次外层调用。
 *
 * 光靠 ALS 传凭据不够稳：宿主派发 `llm/stream` 瀑布时，监听器不一定在我们设置
 * 的那段上下文里被调用（`ctx.waterfall` 有自己的调度）。内层 `createScope()` 拿到的
 * 是**我们自己造的那个 `nested` 对象**，所以直接把凭据挂上去 —— 这条路不经过任何
 * 异步上下文，宿主怎么调度都看得见。
 *
 * 用 Symbol 且不可枚举：它不会出现在 `JSON.stringify`、日志或适配器透传的参数里。
 */
const HINT = Symbol.for('model-failover.billing-attribution.hint')

/** 安装时记下来的参数：`dispatchNested()` 自愈时要用它再装一次。 */
let config = { route: '', enabled: () => true }

/** 自愈的限频：正常路径上每个请求都会走一次 `dispatchNested()`，别真的每次去问桥。 */
const HEAL_INTERVAL_MS = 1_000
let healedAt = 0

/**
 * 计数：这一进程里「外层路由名调用」见过几次、其中几次真的落到了真名下。
 *
 * 它唯一的用途是**让故障可见** —— 以前补丁没装上时，界面与日志都没有任何迹象，
 * 只能靠翻 `ledger.json` 才发现整天都记在路由名上。
 */
const stats = { routed: 0, billed: 0, unbilled: 0, lastRoutedAt: 0, installedAt: 0, lastReason: '' }

/** 访问器是否已经装在 `globalThis` 上。 */
let trapped = false

/** 装访问器之前 `globalThis` 上的那个属性描述符，卸载时照原样放回去。 */
let trapDescriptor

/**
 * 把「发起一次真实模型调用」放进干净上下文里执行。
 *
 * 必须在**模块加载时**就建好 `AsyncResource`：在 `#attempt` 里临时建的话，
 * 它捕获的正是此刻这个「深度=1」的上下文，等于没躲开。
 *
 * 没有外层凭据（桥没装、被用户关掉、测试里直接调适配器）时**原样透传**：这时
 * 内层本来就该被当成嵌套调用，换个上下文反而会把别人的账搅乱。
 *
 * @template T
 * @param {() => T} task
 * @param {object} [hint] 即将发给内层的请求参数。有凭据时会在这个对象上挂一个
 *   不可枚举的 `HINT` 符号，让内层 `createScope()` 无论被谁调度都认得出这一趟
 *   属于哪次外层调用（见 `HINT` 的注释）。
 * @returns {T}
 */
export function dispatchNested(task, hint) {
  const record = link.getStore()
  if (record === undefined) {
    // 没有凭据：要么这一趟不是从本路由发起的（正常），要么补丁被谁拿掉了。
    // 后者以前会一直错下去，这里顺手看一眼，桥在就补上 —— 限频，别每步都查。
    heal()
    return task()
  }
  // 有凭据说明这一趟是本路由发的：但桥有可能在这期间被换掉（cost-meter 卸载后
  // 重装会 `delete` 再赋值，访问器跟着一起没），所以每次也顺手看一眼。便宜。
  heal()
  if (hint !== null && typeof hint === 'object') {
    try {
      Object.defineProperty(hint, HINT, { value: record, configurable: true, enumerable: false, writable: true })
    } catch {
      /* 冻结的参数对象：退回纯 ALS 那条路 */
    }
  }
  return clean.runInAsyncScope(() => link.run(record, task))
}

/**
 * 桥还在、但没被包过时补装一次。异常一律吞掉。
 *
 * 「装访问器」这一步不限频：它只是读一次属性描述符，而且**每次都要看**，因为
 * `dispose()` 的 `delete globalThis[桥]` 会把访问器一起删掉，标志位却还立着。
 * 「包 createScope」那一步按 `HEAL_INTERVAL_MS` 限频，免得每步都重包一遍。
 *
 * @returns {boolean} 这一次真的补装上了才返回 true。
 */
function heal() {
  try {
    trap()
    const now = Date.now()
    if (now - healedAt < HEAL_INTERVAL_MS) return false
    const bridge = globalThis[BRIDGE_KEY]
    if (bridge === null || typeof bridge !== 'object') return false
    if (bridge[WRAPPED] === true) return false
    if (typeof bridge.createScope !== 'function') return false
    healedAt = now
    return patch(bridge)
  } catch {
    return false
  }
}

/**
 * 这一次 `createScope()` 属于哪一次外层调用 —— 先看参数上的 `HINT`，再退回 ALS。
 *
 * 两条路都要留：`HINT` 是我们自己塞进内层参数里的（不依赖宿主怎么调度），ALS 则
 * 覆盖「别的插件照抄我们的参数对象再发一次」这类边角情况。
 *
 * @param {object} scopeOptions
 * @returns {{billed: boolean} | undefined}
 */
function recordOf(scopeOptions) {
  try {
    const hint = scopeOptions?.[HINT]
    if (hint !== null && typeof hint === 'object') return hint
  } catch {
    /* 取不到就当没有 */
  }
  return link.getStore()
}

/**
 * 把 `bridge.createScope` 换成「外层透明、内层放行」的版本。
 *
 * @param {object} bridge
 * @returns {boolean}
 */
function patch(bridge) {
  if (bridge === null || typeof bridge !== 'object') return false
  if (bridge[WRAPPED] === true) return true
  const original = bridge.createScope
  if (typeof original !== 'function') return false
  /** 路由名在**调用时**才读：访问器可能在本插件知道路由名之前就把桥包好了
   *  （`globalThis[桥] = api` 那一刻），那时 `config.route` 还是空的 ——
   *  把它按值捕获下来，整条包装就永远认不出「这一趟是不是本路由」，等于没装。 */
  const routeOf = () => config.route
  /** 问一次「用户还开着这个开关吗」。它自己抛错就当**开着** —— 读不到设置时，
   *  宁可继续按真名记账（最坏也就是退回原行为），也别因为一次读失败把整天的账
   *  都记到路由名上。 */
  const active = () => {
    try {
      return config.enabled() !== false
    } catch {
      return true
    }
  }
  try {
    bridge.createScope = function createScope(scopeOptions, startedAtMs) {
      const routed = scopeOptions !== null && typeof scopeOptions === 'object' && scopeOptions.provider === routeOf()
      try {
        // 不是这条路由，或者用户关掉了：原样交给 cost-meter。
        if (!routed || !active()) {
          const scope = original.call(this, scopeOptions, startedAtMs)
          // 内层（真实候选）的那一次：在凭据上记一笔「这一轮确实落下过真实模型」，
          // 外层收尾时据此才敢说自己已经覆盖。
          //
          // 凭据有两条路：参数上的 `HINT`（我们自己造的那个 `nested` 对象，宿主
          // 怎么调度都看得见）和 ALS（同一段异步上下文时才有）。两条任一命中即可。
          if (!routed && scope !== null && typeof scope === 'object') {
            const current = recordOf(scopeOptions)
            if (current !== undefined) current.billed = true
          }
          return scope
        }
        // 外层（路由名）调用：不建 scope、不写元数据，收尾由下面的 `finish` 回话。
        stats.routed += 1
        stats.lastRoutedAt = Date.now()
        const record = { billed: false }
        return {
          // cost-meter 把每一次 `iterator.next()` 都包在 `proxy.run(…)` 里 ——
          // 这里顺势把凭据挂进上下文，好让内层那次 createScope 认得出是谁。
          run: (operation) => link.run(record, operation),
          observe: () => {},
          async finish() {
            if (record.billed === true) stats.billed += 1
            else stats.unbilled += 1
            return record.billed === true
          },
        }
      } catch {
        // 包一层出任何意外：退回原行为，绝不让计费整条断掉。
        return original.call(this, scopeOptions, startedAtMs)
      }
    }
    bridge[WRAPPED] = true
    bridge[RESTORE] = original
    stats.installedAt = Date.now()
    return true
  } catch {
    return false
  }
}

/**
 * 在 `globalThis` 上装一个访问器，让之后每一次给桥 Symbol 赋值都被顺手包好。
 *
 * cost-meter 的 `dispose()` 是 `delete globalThis[bridgeKey]`，访问器设了
 * `configurable: true` 所以删得掉；它下次装桥时赋值又会经过 setter，自动补上。
 *
 * @returns {boolean}
 */
function trap() {
  // 光看 `trapped` 不够：cost-meter 的 `dispose()` 是 `delete globalThis[桥]`，
  // 访问器会跟着一起消失，而标志位还立着。所以每次都要看**属性本身**还在不在。
  if (trapped) {
    const descriptor = Object.getOwnPropertyDescriptor(globalThis, BRIDGE_KEY)
    if (typeof descriptor?.set === 'function') return true
    trapped = false
  }
  try {
    trapDescriptor = Object.getOwnPropertyDescriptor(globalThis, BRIDGE_KEY)
    let current = globalThis[BRIDGE_KEY]
    Object.defineProperty(globalThis, BRIDGE_KEY, {
      configurable: true,
      enumerable: false,
      get() {
        return current
      },
      set(value) {
        current = value
        try {
          patch(value)
        } catch {
          /* 装不上就保持原样 */
        }
      },
    })
    trapped = true
    if (current !== undefined) patch(current)
    return true
  } catch {
    trapped = false
    return false
  }
}

/**
 * 包一层 cost-meter 的 `createScope`。幂等，失败不抛。
 *
 * @param {string} route - 本插件注册的路由名（`model-failover`）。
 * @param {{enabled?: () => boolean}} [options]
 *   `enabled` 每次外层调用都问一遍：用户刚在设置页关掉「按真实模型计费」时，
 *   下一次调用就该退回原来的行为，不需要重启。**注意**它必须与
 *   `dispatchNested()` 联动 —— 关掉时这里不建空壳，于是 `dispatchNested()`
 *   拿不到凭据、内层也不会逃逸，两件事一起退回原样，不会只做一半而双计。
 * @returns {{ok: boolean, reason: string}}
 */
export function ensureBillingAttribution(route, options = {}) {
  if (typeof route === 'string' && route !== '') config.route = route
  if (typeof options.enabled === 'function') config.enabled = options.enabled
  trap()
  const bridge = globalThis[BRIDGE_KEY]
  if (bridge === null || typeof bridge !== 'object') {
    stats.lastReason = 'no-bridge'
    return { ok: false, reason: 'no-bridge' }
  }
  if (bridge[WRAPPED] === true) {
    stats.lastReason = 'already'
    return { ok: true, reason: 'already' }
  }
  if (typeof bridge.createScope !== 'function') {
    stats.lastReason = 'no-create-scope'
    return { ok: false, reason: 'no-create-scope' }
  }
  const ok = patch(bridge)
  stats.lastReason = ok ? 'installed' : 'patch-failed'
  return ok ? { ok: true, reason: 'installed' } : { ok: false, reason: 'patch-failed' }
}

/**
 * 把桥还原回去（插件卸载 / 停用「按真实模型计费」时）。
 *
 * 包在别人对象上的补丁必须能收回：cost-meter 是常驻插件，它不该被本插件
 * 卸载后还留着一个已经没人在维护的壳。
 *
 * @returns {boolean} 真的还原了才返回 true。
 */
export function releaseBillingAttribution() {
  let released = false
  try {
    if (trapped) {
      // 先把访问器换成普通数据属性，再把包过的函数放回去 —— 顺序不能反：
      // 反过来的话 setter 会立刻把刚还原的函数又包一层。
      const bridge = globalThis[BRIDGE_KEY]
      trapped = false
      if (bridge === undefined) delete globalThis[BRIDGE_KEY]
      // 还原成数据属性时要用**此刻**的桥，不能用装访问器那一刻记下来的那个：
      // cost-meter 中途卸载又重装的话，`trapDescriptor.value` 是已经 dispose 的
      // 旧对象，照它还原等于把新桥丢了。
      else if (trapDescriptor !== undefined && 'value' in trapDescriptor) Object.defineProperty(globalThis, BRIDGE_KEY, { ...trapDescriptor, value: bridge })
      else Object.defineProperty(globalThis, BRIDGE_KEY, trapDescriptor ?? { configurable: true, enumerable: false, writable: true, value: bridge })
      trapDescriptor = undefined
      if (bridge !== null && typeof bridge === 'object' && bridge[WRAPPED] === true) {
        const original = bridge[RESTORE]
        if (typeof original === 'function') bridge.createScope = original
        delete bridge[WRAPPED]
        delete bridge[RESTORE]
        released = true
      }
      return released
    }
    const bridge = globalThis[BRIDGE_KEY]
    if (bridge === null || typeof bridge !== 'object') return false
    if (bridge[WRAPPED] !== true) return false
    const original = bridge[RESTORE]
    if (typeof original === 'function') bridge.createScope = original
    delete bridge[WRAPPED]
    delete bridge[RESTORE]
    return true
  } catch {
    return false
  }
}

/**
 * 这一进程里计费归属的现状 —— 给 `/state` 与设置页用。
 *
 * `routed > 0 && billed === 0` 就是「补丁没生效」的指纹：那时账单会退回路由名，
 * 用户能一眼看见，而不用去翻账本。
 *
 * @returns {{attached: boolean, routed: number, billed: number, unbilled: number, lastRoutedAt: number, installedAt: number, reason: string}}
 */
export function billingAttributionStatus() {
  // 读状态的时候顺手补一眼：`/state` 是定时轮询的，等于又多一层保险 ——
  // 桥在某个没人派发的窗口里被换掉，也能在下一次轮询时自己接回去。
  try {
    heal()
  } catch {
    /* 补不上就照实报「没接管」 */
  }
  let attached = false
  try {
    const bridge = globalThis[BRIDGE_KEY]
    attached = bridge !== null && typeof bridge === 'object' && bridge[WRAPPED] === true
  } catch {
    attached = false
  }
  return {
    attached,
    routed: stats.routed,
    billed: stats.billed,
    unbilled: stats.unbilled,
    lastRoutedAt: stats.lastRoutedAt,
    installedAt: stats.installedAt,
    reason: stats.lastReason,
  }
}

/** 测试用：把计数清零，免得用例之间互相看见对方的数字。 */
export function resetBillingAttributionStats() {
  stats.routed = 0
  stats.billed = 0
  stats.unbilled = 0
  stats.lastRoutedAt = 0
  stats.installedAt = 0
  stats.lastReason = ''
  healedAt = 0
}
