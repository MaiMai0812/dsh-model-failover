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
 * @returns {T}
 */
export function dispatchNested(task) {
  const record = link.getStore()
  if (record === undefined) return task()
  return clean.runInAsyncScope(() => link.run(record, task))
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
  const bridge = globalThis[BRIDGE_KEY]
  if (bridge === null || typeof bridge !== 'object') return { ok: false, reason: 'no-bridge' }
  if (bridge[WRAPPED] === true) return { ok: true, reason: 'already' }
  const original = bridge.createScope
  if (typeof original !== 'function') return { ok: false, reason: 'no-create-scope' }
  const enabled = typeof options.enabled === 'function' ? options.enabled : () => true
  /** 问一次「用户还开着这个开关吗」。它自己抛错就当关掉 —— 不能让它把计费打断。 */
  const active = () => {
    try {
      return enabled() !== false
    } catch {
      return false
    }
  }
  try {
    bridge.createScope = function createScope(scopeOptions, startedAtMs) {
      const routed = scopeOptions !== null && typeof scopeOptions === 'object' && scopeOptions.provider === route
      try {
        // 不是这条路由，或者用户关掉了：原样交给 cost-meter。
        if (!routed || !active()) {
          const scope = original.call(this, scopeOptions, startedAtMs)
          // 内层（真实候选）的那一次：在凭据上记一笔「这一轮确实落下过真实模型」，
          // 外层收尾时据此才敢说自己已经覆盖。
          if (!routed && scope !== null && typeof scope === 'object') {
            const current = link.getStore()
            if (current !== undefined) current.billed = true
          }
          return scope
        }
        // 外层（路由名）调用：不建 scope、不写元数据，收尾由下面的 `finish` 回话。
        const record = { billed: false }
        return {
          // cost-meter 把每一次 `iterator.next()` 都包在 `proxy.run(…)` 里 ——
          // 这里顺势把凭据挂进上下文，好让内层那次 createScope 认得出是谁。
          run: (operation) => link.run(record, operation),
          observe: () => {},
          async finish() {
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
    return { ok: true, reason: 'installed' }
  } catch (error) {
    return { ok: false, reason: String(error?.message ?? error) }
  }
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
  const bridge = globalThis[BRIDGE_KEY]
  if (bridge === null || typeof bridge !== 'object') return false
  if (bridge[WRAPPED] !== true) return false
  const original = bridge[RESTORE]
  try {
    if (typeof original === 'function') bridge.createScope = original
    delete bridge[WRAPPED]
    delete bridge[RESTORE]
    return true
  } catch {
    return false
  }
}
