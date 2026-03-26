/**
 * WriteBatcher — collects SQL write operations from concurrent workers
 * and flushes them in combined statements to reduce SxT API calls.
 *
 * Each queue is independent. Workers await push*() which resolves once
 * the items have been flushed (or rejects on flush error).
 */

export class WriteBatcher {
  /**
   * @param {Function} executeSqlFn — bound (sql) => executeSql(apiUrl, jwt, biscuit, sql)
   * @param {string} schema — sanitized schema name
   * @param {object} opts
   * @param {number} opts.flushIntervalMs — timer-based flush interval (default 5000)
   * @param {number} opts.flushThreshold — count-based flush threshold per queue (default 10)
   */
  constructor(executeSqlFn, schema, { flushIntervalMs = 5000, flushThreshold = 10 } = {}) {
    this._executeSqlFn = executeSqlFn
    this._schema = schema
    this._flushThreshold = flushThreshold

    // Each queue: { items: [], waiters: [] }
    this._queues = {
      evals: { items: [], waiters: [] },
      dealDeletes: { items: [], waiters: [] },
      deals: { items: [], waiters: [] },
      contactDeletes: { items: [], waiters: [] },
      contacts: { items: [], waiters: [] },
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

  /** Flush all pending queues and clear the timer. Called at pipeline end. */
  async drain() {
    clearInterval(this._timer)
    await this._flushAll()
  }

  /** Clear the timer without flushing. For cleanup on fatal error. */
  stop() {
    clearInterval(this._timer)
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
      for (const w of waiters) w.reject(err)
    }
  }

  /** Build and execute SQL for a given queue */
  async _executeQueue(queueName, items) {
    const s = this._schema

    switch (queueName) {
      case 'evals': {
        const sql = `INSERT INTO ${s}.EMAIL_THREAD_EVALUATIONS (ID, THREAD_ID, AI_EVALUATION_AUDIT_ID, AI_INSIGHT, AI_SUMMARY, IS_DEAL, LIKELY_SCAM, AI_SCORE, CREATED_AT, UPDATED_AT) VALUES ${items.join(', ')} ON CONFLICT (THREAD_ID) DO UPDATE SET AI_EVALUATION_AUDIT_ID = EXCLUDED.AI_EVALUATION_AUDIT_ID, AI_INSIGHT = EXCLUDED.AI_INSIGHT, AI_SUMMARY = EXCLUDED.AI_SUMMARY, IS_DEAL = EXCLUDED.IS_DEAL, LIKELY_SCAM = EXCLUDED.LIKELY_SCAM, AI_SCORE = EXCLUDED.AI_SCORE, UPDATED_AT = CURRENT_TIMESTAMP`
        await this._executeSqlFn(sql)
        break
      }

      case 'dealDeletes': {
        const sql = `DELETE FROM ${s}.DEALS WHERE THREAD_ID IN (${items.join(',')})`
        await this._executeSqlFn(sql)
        break
      }

      case 'deals': {
        const sql = `INSERT INTO ${s}.DEALS (ID, USER_ID, THREAD_ID, EMAIL_THREAD_EVALUATION_ID, DEAL_NAME, DEAL_TYPE, CATEGORY, VALUE, CURRENCY, BRAND, IS_AI_SORTED, CREATED_AT, UPDATED_AT) VALUES ${items.join(', ')} ON CONFLICT (THREAD_ID) DO UPDATE SET EMAIL_THREAD_EVALUATION_ID = EXCLUDED.EMAIL_THREAD_EVALUATION_ID, DEAL_NAME = EXCLUDED.DEAL_NAME, DEAL_TYPE = EXCLUDED.DEAL_TYPE, CATEGORY = EXCLUDED.CATEGORY, VALUE = EXCLUDED.VALUE, CURRENCY = EXCLUDED.CURRENCY, BRAND = EXCLUDED.BRAND, UPDATED_AT = CURRENT_TIMESTAMP`
        await this._executeSqlFn(sql)
        break
      }

      case 'contactDeletes': {
        const sql = `DELETE FROM ${s}.DEAL_CONTACTS WHERE DEAL_ID IN (${items.join(',')})`
        await this._executeSqlFn(sql)
        break
      }

      case 'contacts': {
        const sql = `INSERT INTO ${s}.DEAL_CONTACTS (ID, DEAL_ID, CONTACT_ID, CONTACT_TYPE, NAME, EMAIL, COMPANY, TITLE, PHONE_NUMBER, IS_FAVORITE, CREATED_AT, UPDATED_AT) VALUES ${items.join(', ')}`
        await this._executeSqlFn(sql)
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
          const sql = `UPDATE ${s}.DEAL_STATES SET STATUS = 'deal' WHERE EMAIL_METADATA_ID IN (${allDealIds.join(',')})`
          await this._executeSqlFn(sql)
        }
        if (allNotDealIds.length > 0) {
          const sql = `UPDATE ${s}.DEAL_STATES SET STATUS = 'not_deal' WHERE EMAIL_METADATA_ID IN (${allNotDealIds.join(',')})`
          await this._executeSqlFn(sql)
        }
        break
      }

      case 'batchEvents': {
        const sql = `INSERT INTO ${s}.BATCH_EVENTS (TRIGGER_HASH, BATCH_ID, BATCH_TYPE, EVENT_TYPE, CREATED_AT) VALUES ${items.join(', ')} ON CONFLICT (TRIGGER_HASH) DO UPDATE SET EVENT_TYPE = EXCLUDED.EVENT_TYPE, CREATED_AT = CURRENT_TIMESTAMP`
        await this._executeSqlFn(sql)
        break
      }
    }
  }
}
