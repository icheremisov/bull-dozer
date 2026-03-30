# Design: Job Runtime Methods — Integration Tests

**Date:** 2026-03-30
**Branch:** main

---

## Overview

In the previous session, protected methods for BullMQ job runtime were added to the base `DozerWorkflow` class: `log()`, `clearLogs()`, `updateProgress()`, `changePriority()`, and getters `priority` / `progress`. These need integration test coverage.

This document describes what to test, what production changes are required before writing tests, and the structure of two new test files.

---

## Scope

### In scope

1. Integration tests for all new job runtime methods
2. `keepLogs` option enforcement in `InMemoryWorkflowJob`
3. Abstracting log retrieval to `WorkflowQueueDriver` so tests work against both InMemory and real BullMQ
4. Tests for `_job` context isolation under parallel execution
5. Test for correct context re-establishment after `breakUntil` (re-instantiation path)

### Out of scope

- Tests against a live Redis/BullMQ instance (real-queue integration tests are a follow-up)
- Runtime detection of stateful workflows (by architecture, engine creates a new instance per `run()`)

---

## Production changes required before tests

### 1. `WorkflowQueueDriver` — add `getJobLogs()`

BullMQ exposes log retrieval at the Queue level via `Queue.getJobLogs(jobId)`, not on the Job instance. To make test assertions queue-agnostic (works with both `InMemoryWorkflowQueue` and real `BullMQWorkflowQueue`), add `getJobLogs` to the shared interface.

```
WorkflowQueueDriver.getJobLogs(jobId: string): Promise<{ logs: string[]; count: number }>
```

### 2. `BullMQQueueLike` — add `getJobLogs()`

Extend the loose interface used by `BullMQWorkflowQueue` to include the optional BullMQ queue method:

```
BullMQQueueLike.getJobLogs?(jobId, start?, end?, asc?): Promise<{ logs: string[]; count: number }>
```

### 3. `BullMQWorkflowQueue` — implement `getJobLogs()`

Delegate to the underlying BullMQ queue. Return `{ logs: [], count: 0 }` if the queue does not expose the method (graceful fallback for test doubles).

### 4. `InMemoryWorkflowQueue` — implement `getJobLogs()`

Look up the job in `this.jobs`, cast to `InMemoryWorkflowJob`, read its internal `_logs` array. Return `{ logs: [..._logs], count: _logs.length }`.

`InMemoryWorkflowJob._logs` stays private. No public `getLogs()` is needed on the job class — reading always goes through the queue-level API.

### 5. `InMemoryWorkflowJob.log()` — enforce `keepLogs`

BullMQ trims the log to the last N entries after each `log()` call when `keepLogs > 0`. The Lua script uses `if tonumber(keepLogs) > 0 then LTRIM ... end`, so:

- `keepLogs: 0` or `undefined` → keep all entries (no trim)
- `keepLogs: N` where `N > 0` → keep last N entries after each push

`InMemoryWorkflowJob` currently does not enforce this. Fix: after `push(row)`, apply `slice(-keepLogs)` when `options.keepLogs > 0`.

---

## File 1: `src/dozer-engine-job-methods.spec.ts`

### Workflow fixture

`JobMethodsWorkflow` accepts a structured input and runs all operations in a single `@Step`:

```
input: {
  logs?: string[]          // each entry → this.log(entry)
  progress?: number|object // → this.updateProgress(progress)
  priority?: number        // → this.changePriority({ priority })
  clearAfter?: number      // → this.clearLogs(clearAfter) after logging
}
```

Using one `@Step` ensures side effects happen on first execution and are skipped on replay (correct workflow semantics).

### Test groups

#### `log()` and `clearLogs()`

| Test | What it verifies |
|---|---|
| 3 `log()` calls → `getJobLogs()` returns all 3 in order | Basic log recording |
| `log()` return value equals running entry count (1, 2, 3) | Return value contract |
| `clearLogs()` with no argument → empty log | Clear all entries |
| `clearLogs(2)` after 5 entries → last 2 remain | Partial clear |
| `clearLogs(10)` after 3 entries → all 3 remain | keepLast > actual length |

#### `updateProgress()` and `progress` getter

| Test | What it verifies |
|---|---|
| `updateProgress(50)` → `progress === 50` | Numeric progress |
| `updateProgress({ step: 'uploading', pct: 30 })` → deep equal | Object progress |
| Default `progress === 0` before any update | Initial value |
| Two consecutive updates → getter reflects the last one | Overwrite |

#### `changePriority()` and `priority` getter

| Test | What it verifies |
|---|---|
| Default `priority === 0` before any change | Initial value |
| `changePriority({ priority: 5 })` → `priority === 5` | Priority change |
| `changePriority({ priority: 0 })` after 5 → `priority === 0` | Reset to 0 |

#### `keepLogs` option

The `keepLogs` option is set via `@Workflow({ job: { keepLogs: N } })` and flows into `InMemoryWorkflowJob` through `options`.

| Test | What it verifies |
|---|---|
| `keepLogs: 0`, 10 `log()` calls → all 10 entries | 0 = unlimited |
| `keepLogs: 5`, 10 `log()` calls → last 5 entries | Trim to N |
| `keepLogs: 3`, exactly 3 `log()` calls → all 3 entries | Boundary: count === keepLogs |
| `keepLogs: undefined`, 10 `log()` calls → all 10 entries | undefined = unlimited |

---

## File 2: `src/dozer-engine-concurrency.spec.ts`

### Goal

Assert that:
- Each `engine.run()` produces an independent workflow instance
- Each instance's `_job` is bound to its own job, not shared
- Parallel execution via `Promise.all()` produces no cross-contamination of logs or progress

### Workflow fixture

```
ConcurrentWorkflow
  input: { id: string; wakeUpAt?: number }

  @Step('before')
  before(id: string): Promise<string>
    log(`before:${id}`)
    updateProgress({ phase: 'before', id })
    changePriority({ priority: 1 })
    return id

  @Step('after')
  after(id: string): Promise<string>
    log(`after:${id}`)
    updateProgress({ phase: 'after', id })
    return id

  run(input):
    id = await before(input.id)
    if wakeUpAt: breakUntil(wakeUpAt)
    await after(id)
```

### Test 1 — Parallel `_job` isolation

**Scenario:** 10 jobs, no pause (`wakeUpAt` omitted).

```
1. Start 10 jobs: id = 'job-0' … 'job-9'
2. Promise.all(jobIds.map(id => engine.run(id)))
3. For each job:
   getJobLogs(jobId) → ['before:job-N', 'after:job-N'] exactly
   progress          → { phase: 'after', id: 'job-N' }
   priority          → 1
```

**What it proves:** each workflow instance operated on its own `_job`. No entry from another job appears in any log.

### Test 2 — Correct context after `breakUntil` re-instantiation

**Scenario:** 1 job with a short `breakUntil` pause.

```
1. Start job: id = 'resume-test', wakeUpAt = Date.now() + 10
2. engine.run(jobId) → throws DelayedError (caught in test)
3. Assert: getJobLogs(jobId) → ['before:resume-test']  (log before pause was written)
4. await new Promise(r => setTimeout(r, 20))            (let time pass)
5. queue.promoteDelayed(jobId)
6. engine.run(jobId) → completes successfully
7. Assert: getJobLogs(jobId) → ['before:resume-test', 'after:resume-test']
           progress → { phase: 'after', id: 'resume-test' }
```

**What it proves:** when a new instance is created for the second run, `_setJobContext(job)` is called correctly. The new instance writes to the same job object — logs and progress accumulate correctly across the two executions.

---

## File structure after changes

```
src/
  queue/
    workflow-queue.ts            ← add getJobLogs() to WorkflowQueueDriver + BullMQQueueLike
    bullmq-workflow-queue.ts     ← implement getJobLogs() via BullMQ queue
    in-memory-workflow-queue.ts  ← implement getJobLogs() + keepLogs enforcement in log()
  dozer-engine-job-methods.spec.ts   ← new
  dozer-engine-concurrency.spec.ts   ← new
```

---

## Acceptance criteria

- [ ] `InMemoryWorkflowJob.log()` trims according to `keepLogs`
- [ ] `WorkflowQueueDriver.getJobLogs()` implemented in both `BullMQWorkflowQueue` and `InMemoryWorkflowQueue`
- [ ] All tests in `dozer-engine-job-methods.spec.ts` pass
- [ ] All tests in `dozer-engine-concurrency.spec.ts` pass
- [ ] `npm run build` clean
- [ ] `npm test` — 0 failed
