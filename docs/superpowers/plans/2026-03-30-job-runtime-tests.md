# Job Runtime Methods — Integration Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `getJobLogs` to `WorkflowQueueDriver`, enforce `keepLogs` in `InMemoryWorkflowJob`, and write integration tests for all job runtime methods (`log`, `clearLogs`, `updateProgress`, `changePriority`) plus parallel context isolation.

**Architecture:** Three production-code changes (`workflow-queue.ts`, `bullmq-workflow-queue.ts`, `in-memory-workflow-queue.ts`) unlock queue-agnostic log reading via `queue.getJobLogs(jobId)`. Two new spec files cover method behaviour and concurrent execution isolation.

**Tech Stack:** NestJS testing utilities (`@nestjs/testing`), Jest, `InMemoryWorkflowQueue`, existing `DozerModule`/`DozerEngine` patterns.

---

## File Map

| File | Change |
|---|---|
| `src/queue/workflow-queue.ts` | Add `getJobLogs` to `WorkflowQueueDriver`; add optional `getJobLogs` to `BullMQQueueLike` |
| `src/queue/bullmq-workflow-queue.ts` | Implement `getJobLogs` delegating to BullMQ queue |
| `src/queue/in-memory-workflow-queue.ts` | Enforce `keepLogs` in `log()`; init `priority` from options; add `getLogs()` helper; implement `getJobLogs` on queue |
| `src/dozer-engine-job-methods.spec.ts` | New: tests for `log`, `clearLogs`, `updateProgress`, `changePriority`, `keepLogs` |
| `src/dozer-engine-concurrency.spec.ts` | New: tests for parallel `_job` isolation and re-instantiation after `breakUntil` |

---

## Task 1: Add `getJobLogs` to shared interfaces

**Files:**
- Modify: `src/queue/workflow-queue.ts`

- [ ] **Step 1: Add `getJobLogs` to `WorkflowQueueDriver`**

Open `src/queue/workflow-queue.ts`. Find the `WorkflowQueueDriver` interface and add the new method:

```typescript
export interface WorkflowQueueDriver {
  add<TInput = unknown>(
    workflowName: string,
    data: WorkflowJobData<TInput>,
    options?: WorkflowJobOptions,
  ): Promise<WorkflowJob<TInput>>;
  get<TInput = unknown>(jobId: string): Promise<WorkflowJob<TInput> | null>;
  moveToDelayed(
    jobId: string,
    timestamp: number,
    token?: string,
  ): Promise<void>;
  promoteDelayed(jobId: string): Promise<void>;
  getJobLogs(jobId: string): Promise<{ logs: string[]; count: number }>;
}
```

- [ ] **Step 2: Add optional `getJobLogs` to `BullMQQueueLike`**

In the same file, find `BullMQQueueLike` and add the optional method:

```typescript
export interface BullMQQueueLike<TData> {
  add(
    name: string,
    data: TData,
    options?: WorkflowJobOptions,
  ): Promise<BullMQJobLike<unknown>>;
  getJob(jobId: string): Promise<BullMQJobLike<unknown> | null | undefined>;
  getJobLogs?(
    jobId: string,
    start?: number,
    end?: number,
    asc?: boolean,
  ): Promise<{ logs: string[]; count: number }>;
}
```

- [ ] **Step 3: Verify TypeScript reports errors for unimplemented interface**

```bash
cd /Volumes/Storage/Flutter/dozer && npx tsc -p tsconfig.build.json --noEmit 2>&1 | head -20
```

Expected: errors saying `BullMQWorkflowQueue` and `InMemoryWorkflowQueue` are missing `getJobLogs`. This confirms the interface change is wired correctly.

---

## Task 2: Implement `getJobLogs` in `BullMQWorkflowQueue`

**Files:**
- Modify: `src/queue/bullmq-workflow-queue.ts`

- [ ] **Step 1: Add `getJobLogs` to `BullMQWorkflowQueue`**

Open `src/queue/bullmq-workflow-queue.ts`. Add after `promoteDelayed`:

```typescript
async getJobLogs(
  jobId: string,
): Promise<{ logs: string[]; count: number }> {
  return (await this.queue.getJobLogs?.(jobId)) ?? { logs: [], count: 0 };
}
```

The `?.` handles queue adapters (e.g. test doubles) that don't implement `getJobLogs`.

---

## Task 3: Update `InMemoryWorkflowJob` and `InMemoryWorkflowQueue`

**Files:**
- Modify: `src/queue/in-memory-workflow-queue.ts`

Three changes to this file:
1. Initialize `priority` from `options.priority` (it was always `0`)
2. Enforce `keepLogs` trimming inside `log()`
3. Add `getLogs(): string[]` helper (used internally by the queue and by tests)
4. Implement `InMemoryWorkflowQueue.getJobLogs()`

- [ ] **Step 1: Replace `InMemoryWorkflowJob` with the updated version**

Replace the entire class (constructor through all methods) with:

```typescript
class InMemoryWorkflowJob<TInput = unknown> implements WorkflowJob<TInput> {
  priority: number;
  progress: number | object = 0;
  private _logs: string[] = [];

  constructor(
    public readonly id: string,
    public readonly name: string,
    public data: WorkflowJobData<TInput>,
    public readonly options?: WorkflowJobOptions,
  ) {
    this.priority = options?.priority ?? 0;
  }

  updateData(data: WorkflowJobData<TInput>): Promise<void> {
    this.data = data;
    return Promise.resolve();
  }

  log(row: string): Promise<number> {
    this._logs.push(row);
    const keepLogs = this.options?.keepLogs;
    if (keepLogs !== undefined && keepLogs > 0 && this._logs.length > keepLogs) {
      this._logs = this._logs.slice(-keepLogs);
    }
    return Promise.resolve(this._logs.length);
  }

  clearLogs(keepLast?: number): Promise<void> {
    this._logs =
      keepLast !== undefined && keepLast > 0
        ? this._logs.slice(-keepLast)
        : [];
    return Promise.resolve();
  }

  changePriority(opts: { priority?: number }): Promise<void> {
    if (opts.priority !== undefined) {
      this.priority = opts.priority;
    }
    return Promise.resolve();
  }

  updateProgress(progress: number | object): Promise<void> {
    this.progress = progress;
    return Promise.resolve();
  }

  /** Test helper — returns a copy of the job's log entries. */
  getLogs(): string[] {
    return [...this._logs];
  }
}
```

- [ ] **Step 2: Add `getJobLogs` to `InMemoryWorkflowQueue`**

Inside `InMemoryWorkflowQueue`, add after `promoteDelayed`:

```typescript
async getJobLogs(
  jobId: string,
): Promise<{ logs: string[]; count: number }> {
  const job = this.jobs.get(jobId) as
    | InMemoryWorkflowJob<unknown>
    | undefined;
  if (!job) {
    return { logs: [], count: 0 };
  }
  const logs = job.getLogs();
  return { logs, count: logs.length };
}
```

---

## Task 4: Build check and commit

- [ ] **Step 1: Verify clean build**

```bash
cd /Volumes/Storage/Flutter/dozer && npm run build 2>&1
```

Expected: no output after the clean step (zero errors).

- [ ] **Step 2: Run existing tests**

```bash
cd /Volumes/Storage/Flutter/dozer && npm test 2>&1 | tail -10
```

Expected:
```
Test Suites: 22 passed, 22 total
Tests:       184 passed, 184 total
```

- [ ] **Step 3: Commit**

```bash
cd /Volumes/Storage/Flutter/dozer && git add src/queue/workflow-queue.ts src/queue/bullmq-workflow-queue.ts src/queue/in-memory-workflow-queue.ts && git commit -m "feat: add getJobLogs to WorkflowQueueDriver, enforce keepLogs in InMemoryWorkflowJob"
```

---

## Task 5: Write `dozer-engine-job-methods.spec.ts`

**Files:**
- Create: `src/dozer-engine-job-methods.spec.ts`

- [ ] **Step 1: Create the file with imports, fixtures, and module setup**

Create `src/dozer-engine-job-methods.spec.ts`:

```typescript
import { Test, TestingModule } from '@nestjs/testing';
import {
  DozerEngine,
  DozerModule,
  DozerWorkflow,
  InMemoryWorkflowQueue,
  Step,
  Workflow,
} from './index';

// ---------------------------------------------------------------------------
// Shared input type
// ---------------------------------------------------------------------------

type JobMethodsInput = {
  logs?: string[];
  progress?: number | object;
  priority?: number;
  clearAfter?: number;
};

// ---------------------------------------------------------------------------
// Workflow fixtures
// ---------------------------------------------------------------------------

@Workflow({ name: 'job-methods-workflow' })
class JobMethodsWorkflow extends DozerWorkflow<JobMethodsInput> {
  @Step({ name: 'execute' })
  async execute(input: JobMethodsInput): Promise<void> {
    for (const row of input.logs ?? []) {
      await this.log(row);
    }
    if (input.clearAfter !== undefined) {
      await this.clearLogs(input.clearAfter);
    }
    if (input.progress !== undefined) {
      await this.updateProgress(input.progress);
    }
    if (input.priority !== undefined) {
      await this.changePriority({ priority: input.priority });
    }
  }

  async run(input: JobMethodsInput): Promise<void> {
    await this.execute(input);
  }
}

// keepLogs variants — keepLogs is baked into the @Workflow decorator
@Workflow({ name: 'log-keep-none' })
class LogKeepNoneWorkflow extends DozerWorkflow<string[]> {
  @Step({ name: 'log-all' })
  async logAll(entries: string[]): Promise<void> {
    for (const row of entries) {
      await this.log(row);
    }
  }

  async run(input: string[]): Promise<void> {
    await this.logAll(input);
  }
}

@Workflow({ name: 'log-keep-0', job: { keepLogs: 0 } })
class LogKeep0Workflow extends DozerWorkflow<string[]> {
  @Step({ name: 'log-all' })
  async logAll(entries: string[]): Promise<void> {
    for (const row of entries) {
      await this.log(row);
    }
  }

  async run(input: string[]): Promise<void> {
    await this.logAll(input);
  }
}

@Workflow({ name: 'log-keep-5', job: { keepLogs: 5 } })
class LogKeep5Workflow extends DozerWorkflow<string[]> {
  @Step({ name: 'log-all' })
  async logAll(entries: string[]): Promise<void> {
    for (const row of entries) {
      await this.log(row);
    }
  }

  async run(input: string[]): Promise<void> {
    await this.logAll(input);
  }
}

@Workflow({ name: 'log-keep-3', job: { keepLogs: 3 } })
class LogKeep3Workflow extends DozerWorkflow<string[]> {
  @Step({ name: 'log-all' })
  async logAll(entries: string[]): Promise<void> {
    for (const row of entries) {
      await this.log(row);
    }
  }

  async run(input: string[]): Promise<void> {
    await this.logAll(input);
  }
}

// ---------------------------------------------------------------------------
// Module setup
// ---------------------------------------------------------------------------

describe('DozerEngine job runtime methods', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature([
          JobMethodsWorkflow,
          LogKeepNoneWorkflow,
          LogKeep0Workflow,
          LogKeep5Workflow,
          LogKeep3Workflow,
        ]),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
  });

  afterEach(async () => {
    await moduleRef?.close();
  });
```

- [ ] **Step 2: Add `log()` and `clearLogs()` tests**

Append to the `describe` block (before the closing `}`):

```typescript
  // -------------------------------------------------------------------------
  // log() / clearLogs()
  // -------------------------------------------------------------------------

  describe('log()', () => {
    it('records entries in order', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        logs: ['alpha', 'beta', 'gamma'],
      });
      await engine.run(jobId);

      const { logs, count } = await queue.getJobLogs(jobId);
      expect(logs).toEqual(['alpha', 'beta', 'gamma']);
      expect(count).toBe(3);
    });

    it('returns the running entry count via getJobLogs().count', async () => {
      // After N log() calls the count should equal N
      const jobId = await engine.start('job-methods-workflow', {
        logs: ['a', 'b'],
      });
      await engine.run(jobId);

      const { count } = await queue.getJobLogs(jobId);
      expect(count).toBe(2);
    });
  });

  describe('clearLogs()', () => {
    it('clears all entries when called without argument', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        logs: ['x', 'y', 'z'],
        clearAfter: undefined, // explicit: we call clearLogs() with no arg
      });
      // We need a workflow variant that calls clearLogs() unconditionally.
      // Use clearAfter: 0 — the step calls clearLogs(0) which means keepLast=0 → clear all.
      const jobId2 = await engine.start('job-methods-workflow', {
        logs: ['x', 'y', 'z'],
        clearAfter: 0,
      });
      await engine.run(jobId2);

      const { logs } = await queue.getJobLogs(jobId2);
      expect(logs).toEqual([]);
    });

    it('retains the last N entries when called with keepLast', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        logs: ['a', 'b', 'c', 'd', 'e'],
        clearAfter: 2,
      });
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toEqual(['d', 'e']);
    });

    it('retains all entries when keepLast exceeds actual count', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        logs: ['a', 'b'],
        clearAfter: 10,
      });
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toEqual(['a', 'b']);
    });
  });
```

- [ ] **Step 3: Add `updateProgress()` and `changePriority()` tests**

Append to the `describe` block:

```typescript
  // -------------------------------------------------------------------------
  // updateProgress() / progress getter
  // -------------------------------------------------------------------------

  describe('updateProgress()', () => {
    it('sets numeric progress on the job', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        progress: 50,
      });
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.progress).toBe(50);
    });

    it('sets object progress on the job', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        progress: { step: 'uploading', pct: 30 },
      });
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.progress).toEqual({ step: 'uploading', pct: 30 });
    });

    it('defaults to 0 before any updateProgress call', async () => {
      const jobId = await engine.start('job-methods-workflow', {});
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.progress).toBe(0);
    });

    it('reflects the last updateProgress value', async () => {
      // Run job twice — second run replays step (cached), so progress stays
      // at value written on first run.
      const jobId = await engine.start('job-methods-workflow', {
        progress: 99,
      });
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.progress).toBe(99);
    });
  });

  // -------------------------------------------------------------------------
  // changePriority() / priority getter
  // -------------------------------------------------------------------------

  describe('changePriority()', () => {
    it('defaults to 0 before any changePriority call', async () => {
      const jobId = await engine.start('job-methods-workflow', {});
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.priority).toBe(0);
    });

    it('updates priority on the job', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        priority: 5,
      });
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.priority).toBe(5);
    });

    it('can reset priority back to 0', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        priority: 0,
      });
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.priority).toBe(0);
    });
  });
```

- [ ] **Step 4: Add `keepLogs` option tests**

Append to the `describe` block (before the closing `}`):

```typescript
  // -------------------------------------------------------------------------
  // keepLogs option
  // -------------------------------------------------------------------------

  describe('keepLogs job option', () => {
    const TEN_ENTRIES = Array.from({ length: 10 }, (_, i) => `entry-${i}`);

    it('keeps all entries when keepLogs is undefined', async () => {
      const jobId = await engine.start('log-keep-none', TEN_ENTRIES);
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toHaveLength(10);
      expect(logs[0]).toBe('entry-0');
      expect(logs[9]).toBe('entry-9');
    });

    it('keeps all entries when keepLogs is 0 (treated as unlimited)', async () => {
      const jobId = await engine.start('log-keep-0', TEN_ENTRIES);
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toHaveLength(10);
    });

    it('trims to last N entries when keepLogs is 5', async () => {
      const jobId = await engine.start('log-keep-5', TEN_ENTRIES);
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toHaveLength(5);
      expect(logs[0]).toBe('entry-5');
      expect(logs[4]).toBe('entry-9');
    });

    it('keeps all entries when count equals keepLogs (boundary: count === keepLogs)', async () => {
      const jobId = await engine.start('log-keep-3', ['a', 'b', 'c']);
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toEqual(['a', 'b', 'c']);
    });
  });
}); // end describe
```

- [ ] **Step 5: Run the new tests**

```bash
cd /Volumes/Storage/Flutter/dozer && npx jest src/dozer-engine-job-methods.spec.ts --no-coverage 2>&1 | tail -20
```

Expected: all tests in this file pass.

- [ ] **Step 6: Commit**

```bash
cd /Volumes/Storage/Flutter/dozer && git add src/dozer-engine-job-methods.spec.ts && git commit -m "test: add integration tests for job runtime methods (log, clearLogs, progress, priority, keepLogs)"
```

---

## Task 6: Write `dozer-engine-concurrency.spec.ts`

**Files:**
- Create: `src/dozer-engine-concurrency.spec.ts`

- [ ] **Step 1: Create the file with imports, fixture, and module setup**

Create `src/dozer-engine-concurrency.spec.ts`:

```typescript
import { Test, TestingModule } from '@nestjs/testing';
import { DelayedError } from 'bullmq';
import {
  DozerEngine,
  DozerModule,
  DozerWorkflow,
  InMemoryWorkflowQueue,
  Step,
  Workflow,
} from './index';

// ---------------------------------------------------------------------------
// Workflow fixture
// ---------------------------------------------------------------------------

@Workflow({ name: 'concurrent-workflow' })
class ConcurrentWorkflow extends DozerWorkflow<{
  id: string;
  wakeUpAt?: number;
}> {
  @Step({ name: 'before' })
  async before(id: string): Promise<string> {
    await this.log(`before:${id}`);
    await this.updateProgress({ phase: 'before', id });
    await this.changePriority({ priority: 1 });
    return id;
  }

  @Step({ name: 'after' })
  async after(id: string): Promise<string> {
    await this.log(`after:${id}`);
    await this.updateProgress({ phase: 'after', id });
    return id;
  }

  async run(input: { id: string; wakeUpAt?: number }): Promise<string> {
    const id = await this.before(input.id);
    if (input.wakeUpAt !== undefined) {
      this.breakUntil(input.wakeUpAt);
    }
    return this.after(id);
  }
}

// ---------------------------------------------------------------------------
// Module setup
// ---------------------------------------------------------------------------

describe('DozerEngine concurrency — _job context isolation', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature([ConcurrentWorkflow]),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
  });

  afterEach(async () => {
    await moduleRef?.close();
  });
```

- [ ] **Step 2: Add the parallel isolation test**

Append to the `describe` block (before closing `}`):

```typescript
  // -------------------------------------------------------------------------
  // Test 1: 10 concurrent jobs — no cross-contamination of _job context
  // -------------------------------------------------------------------------

  it('isolates _job context for each concurrent execution', async () => {
    const ids = Array.from({ length: 10 }, (_, i) => `job-${i}`);

    // Start all 10 jobs
    const jobIds = await Promise.all(
      ids.map((id) => engine.start('concurrent-workflow', { id })),
    );

    // Run all concurrently
    await Promise.all(jobIds.map((jobId) => engine.run(jobId)));

    // Assert each job's logs and progress contain only its own id
    for (let i = 0; i < jobIds.length; i++) {
      const jobId = jobIds[i];
      const id = ids[i];

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toEqual([`before:${id}`, `after:${id}`]);

      const job = await queue.get(jobId);
      expect(job?.progress).toEqual({ phase: 'after', id });
      expect(job?.priority).toBe(1);
    }
  });
```

- [ ] **Step 3: Add the re-instantiation after `breakUntil` test**

Append to the `describe` block (before closing `}`):

```typescript
  // -------------------------------------------------------------------------
  // Test 2: Re-instantiation after breakUntil — context restored correctly
  //
  // On first engine.run(), the workflow logs 'before:X' then hits breakUntil
  // and throws DelayedError. A brand-new workflow instance is created on the
  // second engine.run(). That second instance must receive the same job via
  // _setJobContext so its 'after:X' log lands on the right job.
  // -------------------------------------------------------------------------

  it('re-establishes _job context on a new instance after breakUntil', async () => {
    const wakeUpAt = Date.now() + 10; // 10 ms in the future

    const jobId = await engine.start('concurrent-workflow', {
      id: 'resume-test',
      wakeUpAt,
    });

    // First run: hits breakUntil, throws DelayedError
    await expect(engine.run(jobId)).rejects.toBeInstanceOf(DelayedError);

    // 'before' step executed — log written on first instance
    const { logs: logsAfterFirstRun } = await queue.getJobLogs(jobId);
    expect(logsAfterFirstRun).toEqual(['before:resume-test']);

    // Wait for the wake-up timestamp to pass
    await new Promise((resolve) => setTimeout(resolve, 20));

    await queue.promoteDelayed(jobId);

    // Second run: new instance, breakUntil passes, 'after' step executes
    await engine.run(jobId);

    // Both logs present — written by two different instances to the same job
    const { logs: logsAfterSecondRun } = await queue.getJobLogs(jobId);
    expect(logsAfterSecondRun).toEqual([
      'before:resume-test',
      'after:resume-test',
    ]);

    const job = await queue.get(jobId);
    expect(job?.progress).toEqual({ phase: 'after', id: 'resume-test' });
  });
}); // end describe
```

- [ ] **Step 4: Run the new tests**

```bash
cd /Volumes/Storage/Flutter/dozer && npx jest src/dozer-engine-concurrency.spec.ts --no-coverage 2>&1 | tail -20
```

Expected: both tests pass.

- [ ] **Step 5: Commit**

```bash
cd /Volumes/Storage/Flutter/dozer && git add src/dozer-engine-concurrency.spec.ts && git commit -m "test: add concurrency tests for parallel _job isolation and breakUntil re-instantiation"
```

---

## Task 7: Full test suite verification

- [ ] **Step 1: Run all tests**

```bash
cd /Volumes/Storage/Flutter/dozer && npm test 2>&1 | tail -15
```

Expected:
```
Test Suites: 24 passed, 24 total
Tests:       NNN passed, NNN total
Time:        ...
```

No failed tests. Suite count increased from 22 to 24 (two new files).

- [ ] **Step 2: Verify build is still clean**

```bash
cd /Volumes/Storage/Flutter/dozer && npm run build 2>&1
```

Expected: no output after the clean step.

---

## Self-Review

### Spec coverage

| Spec requirement | Covered by |
|---|---|
| `getJobLogs` in `WorkflowQueueDriver` | Task 1 |
| `getJobLogs` in `BullMQQueueLike` | Task 1 |
| `BullMQWorkflowQueue.getJobLogs()` | Task 2 |
| `keepLogs` enforcement in `InMemoryWorkflowJob.log()` | Task 3 |
| `getLogs()` internal helper on `InMemoryWorkflowJob` | Task 3 |
| `InMemoryWorkflowQueue.getJobLogs()` | Task 3 |
| `priority` initialized from `options.priority` | Task 3 |
| `log()` / `clearLogs()` tests | Task 5 |
| `updateProgress()` / `progress` tests | Task 5 |
| `changePriority()` / `priority` tests | Task 5 |
| `keepLogs: 0` = unlimited | Task 5 |
| `keepLogs: 5` = trim to last 5 | Task 5 |
| `keepLogs: 3`, count === keepLogs boundary | Task 5 |
| `keepLogs: undefined` = unlimited | Task 5 |
| 10 parallel jobs, no cross-contamination | Task 6 |
| Re-instantiation after `breakUntil` | Task 6 |

All requirements covered. ✓

### Type consistency

- `getJobLogs` returns `Promise<{ logs: string[]; count: number }>` in interface, both implementations, and all test assertions. ✓
- `getLogs()` on `InMemoryWorkflowJob` returns `string[]`; `getJobLogs` on queue wraps it in `{ logs, count }`. ✓
- `changePriority` accepts `{ priority?: number; lifo?: boolean }` in `WorkflowJob` interface and `DozerWorkflow` protected method. Task 5 uses `{ priority: N }` (subset). ✓
- `clearLogs(0)` in test uses `clearAfter: 0` in input — the step calls `this.clearLogs(0)`. `clearLogs(0)` in `InMemoryWorkflowJob`: `keepLast = 0`, condition `keepLast > 0` is false → `this._logs = []`. All clear. ✓
