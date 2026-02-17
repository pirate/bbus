import assert from 'node:assert/strict'
import { test } from 'node:test'

import { AsyncLock } from '../src/lock_manager.js'

test('AsyncLock(1): releasing to a queued waiter does not allow a new acquire to slip in', async () => {
  const lock = new AsyncLock(1)

  await lock.acquire() // Initial holder.

  const waiter = lock.acquire()
  assert.equal(lock.waiters.length, 1)

  // Transfer the permit to the waiter.
  lock.release()

  // A new acquire in the same tick must wait behind the queued waiter.
  let contender_acquired = false
  const contender = lock.acquire().then(() => {
    contender_acquired = true
  })
  assert.equal(lock.waiters.length, 1)

  await waiter
  await Promise.resolve()
  assert.equal(contender_acquired, false)
  lock.release() // waiter release
  await contender
  lock.release() // contender release

  assert.equal(lock.in_use, 0)
})
