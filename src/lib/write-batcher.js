/**
 * WriteBatcher — collects SQL write operations from concurrent workers
 * and flushes them in combined statements to reduce SxT API calls.
 *
 * Each queue is independent. Workers await push*() which resolves once
 * the items have been flushed (or rejects on flush error).
 */

import {
  evaluations as evalSql,
  deals as dealsSql,
  dealContacts as dealContactsSql,
  contacts as contactsSql,
  batchEvents as batchEventsSql,
  dealStates as dealStatesSql,
} from './sql/index.js'

export class WriteBatcher {
  /**
   * @param {Function} executeSqlFn — bound (sql) => executeSql(apiUrl, jwt, biscuit, sql)
   * @param {string} schema — sanitized schema name
   * @param {object} opts
   * @param {number} opts.flushIntervalMs — timer-based flush interval (default 5000)
   * @param {number} opts.flushThreshold — count-based flush threshold per queue (default 10)
   */
  constructor(executeSqlFn, schema, { flushIntervalMs = 5000, flushThreshold = 10, coreSchema = 'EMAIL_CORE_STAGING' } = {}) {
    this._executeSqlFn = executeSqlFn
    this._schema = schema
    this._coreSchema = coreSchema
    this._flushThreshold = flushThreshold

    // Each queue: { items: [], waiters: [] }
    this._queues = {
      evals: { items: [], waiters: [] },
      dealDeletes: { items: [], waiters: [] },
      deals: { items: [], waiters: [] },
      contactDeletes: { items: [], waiters: [] },
      contacts: { items: [], waiters: [] },
      coreContacts: { items: [], waiters: [] },
      stateUpdates: { items: [], waiters: [] },
      batchEvents: { items: [], waiters: [] },
    }

    this._timer = setInterval(() => this._flushAll(), flushIntervalMs)
  }

  // ===========================================================
  // Push methods
  // ===========================================================

  /** Push pre-built VALUES strings for EMAIL_THREAD_EVALUATIONS upsert */
  pushEvals(rows) {
    return this._push('evals', rows)
  }

  /** Push sanitized thread IDs for DEALS delete */
  pushDealDeletes(threadIds) {
    return this._push('dealDeletes', threadIds)
  }

  /** Push pre-built VALUES strings for DEALS upsert */
  pushDeals(rows) {
    return this._push('deals', rows)
  }

  /** Push sanitized deal IDs for DEAL_CONTACTS delete */
  pushContactDeletes(dealIds) {
    return this._push('contactDeletes', dealIds)
  }

  /** Push pre-built VALUES strings for DEAL_CONTACTS insert */
  pushContacts(rows) {
    return this._push('contacts', rows)
  }

  /** Push pre-built VALUES strings for core contacts upsert (COALESCE ON CONFLICT) */
  pushCoreContacts(rows) {
    return this._push('coreContacts', rows)
  }

  /**
   * Push state update IDs.
   * @param {string[]} dealEmailIds — email metadata IDs for 'deal' status
   * @param {string[]} notDealEmailIds — email metadata IDs for 'not_deal' status
   */
  pushStateUpdates(dealEmailIds, notDealEmailIds) {
    return this._push('stateUpdates', [{ dealEmailIds, notDealEmailIds }])
  }

  /** Push pre-built VALUES strings for BATCH_EVENTS upsert */
  pushBatchEvents(rows) {
    return this._push('batchEvents', rows)
  }

  // ===========================================================
  // Lifecycle
  // ===========================================================

  /** Flush all pending queues and clear the timer. Called at pipeline end. Loops until all queues are empty. */
  async drain() {
    clearInterval(this._timer)
    let hasItems = true
    while (hasItems) {
      await this._flushAll()
      hasItems = Object.values(this._queues).some((q) => q.items.length > 0)
    }
  }

  /** Clear the timer without flushing. Rejects all pending waiters. For cleanup on fatal error. */
  stop() {
    clearInterval(this._timer)
    const err = new Error('WriteBatcher stopped')
    for (const queue of Object.values(this._queues)) {
      const waiters = queue.waiters
      queue.waiters = []
      queue.items = []
      for (const w of waiters) w.reject(err)
    }
  }

  // ===========================================================
  // Internal
  // ===========================================================

  /**
   * Add items to a queue, register a waiter, and trigger flush if threshold met.
   * @returns {Promise<void>} resolves when the items have been flushed
   */
  _push(queueName, items) {
    const queue = this._queues[queueName]
    queue.items.push(...items)

    const promise = new Promise((resolve, reject) => {
      queue.waiters.push({ resolve, reject })
    })

    if (queue.items.length >= this._flushThreshold) {
      this._flushQueue(queueName)
    }

    return promise
  }

  /** Flush all queues that have pending items */
  async _flushAll() {
    const promises = []
    for (const name of Object.keys(this._queues)) {
      if (this._queues[name].items.length > 0) {
        promises.push(this._flushQueue(name))
      }
    }
    await Promise.all(promises)
  }

  /** Flush a single queue */
  async _flushQueue(queueName) {
    const queue = this._queues[queueName]

    // Swap out items and waiters atomically
    const items = queue.items
    const waiters = queue.waiters
    queue.items = []
    queue.waiters = []

    if (items.length === 0) return

    console.log(`[write-batcher] flushing ${queueName}: ${items.length} items`)

    try {
      await this._executeQueue(queueName, items)
      for (const w of waiters) w.resolve()
    } catch (err) {
      console.error(`[write-batcher] ${queueName} flush failed (${items.length} items): ${err.message}`)
      // If combined flush fails, try each item individually to isolate the bad one
      if (items.length > 1 && err.message.includes('SxT 400')) {
        console.error(
          `[write-batcher] combined ${queueName} flush failed, falling back to individual items`,
        )
        for (let i = 0; i < items.length; i++) {
          try {
            await this._executeQueue(queueName, [items[i]])
          } catch (itemErr) {
            console.error(`[write-batcher] ${queueName} item ${i} failed: ${itemErr.message}`)
          }
        }
        // Resolve all waiters — individual items that succeeded are written,
        // failed items are logged and skipped
        for (const w of waiters) w.resolve()
      } else {
        for (const w of waiters) w.reject(err)
      }
    }
  }

  /** Build and execute SQL for a given queue */
  async _executeQueue(queueName, items) {
    const s = this._schema

    switch (queueName) {
      case 'evals': {
        await this._executeSqlFn(evalSql.upsert(s, items))
        break
      }

      case 'dealDeletes': {
        await this._executeSqlFn(dealsSql.deleteByThreadIds(s, items))
        break
      }

      case 'deals': {
        await this._executeSqlFn(dealsSql.upsert(s, items))
        break
      }

      case 'contactDeletes': {
        await this._executeSqlFn(dealContactsSql.deleteByDealIds(s, items))
        break
      }

      case 'contacts': {
        await this._executeSqlFn(dealContactsSql.upsert(s, items))
        break
      }

      case 'coreContacts': {
        // Dedup by (USER_ID, EMAIL) — concurrent workers may push the same contact
        const dedupMap = new Map()
        for (const item of items) {
          const m = item.match(/^\('([^']*(?:''[^']*)*)',\s*'([^']*(?:''[^']*)*)'/)
          const key = m ? `${m[1]}|${m[2]}` : item
          dedupMap.set(key, item) // last write wins
        }
        const uniqueItems = [...dedupMap.values()]
        if (uniqueItems.length < items.length) {
          console.log(`[write-batcher] coreContacts deduped: ${items.length} → ${uniqueItems.length}`)
        }
        const cs = this._coreSchema
        await this._executeSqlFn(contactsSql.upsert(cs, uniqueItems))
        break
      }

      case 'stateUpdates': {
        // Merge all stateUpdate items into combined ID lists
        const allDealIds = []
        const allNotDealIds = []
        for (const item of items) {
          allDealIds.push(...item.dealEmailIds)
          allNotDealIds.push(...item.notDealEmailIds)
        }
        if (allDealIds.length > 0) {
          await this._executeSqlFn(dealStatesSql.updateStatusByIds(s, allDealIds, 'deal'))
        }
        if (allNotDealIds.length > 0) {
          await this._executeSqlFn(dealStatesSql.updateStatusByIds(s, allNotDealIds, 'not_deal'))
        }
        break
      }

      case 'batchEvents': {
        await this._executeSqlFn(batchEventsSql.upsertBulk(s, items))
        break
      }
    }
  }
}
