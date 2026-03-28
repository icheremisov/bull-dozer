# Workflow Failure Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-workflow `onFailed()` method, global `onWorkflowFailed` module callback, and opt-in failure result queue publishing to Dozer.

**Architecture:** All failure side-effects are consolidated in a new private `DozerEngine.handleWorkflowFailure()` method, called after `stateStore.markFailed()` in the terminal failure branch of `engine.run()`. The result queue payload shape gains `status`/`error` fields (breaking change). Version bumps to 0.6.0.

**Tech Stack:** TypeScript, NestJS, BullMQ, Jest (`npm test` / `npx jest --testPathPattern=dozer-engine`)

**Spec:** `docs/superpowers/specs/2026-03-28-workflow-failure-handling-design.md`

---

## File Map

| File | Action |
|---|---|
| `src/workflow/workflow-with-failure-handler.ts` | **Create** — `WorkflowWithFailureHandler<TInput>` interface |
| `src/queue/workflow-queue.ts` | **Modify** — add `status`, `error` to `WorkflowResultQueueJobData` |
| `src/decorators/workflow.decorator.ts` | **Modify** — add `publishOnFailure` to `WorkflowResultQueueOptions` |
| `src/client/workflow-result-worker.ts` | **Modify** — add `status`, `error` to `WorkflowResultMessage`; update `decodeWorkflowResultJob` |
| `src/dozer.module.ts` | **Modify** — add `onWorkflowFailed` to `DozerModuleOptions` |
| `src/engine/dozer-engine.ts` | **Modify** — add `handleWorkflowFailure`, track `lastWorkflow`/`lastInput`, update catch block, update `enqueueWorkflowResult` |
| `src/index.ts` | **Modify** — export `WorkflowWithFailureHandler` |
| `src/dozer-engine.spec.ts` | **Modify** — fix existing assertions + add new test cases |
| `package.json` | **Modify** — version `0.5.0` → `0.6.0` |

---

## Task 1: Create `WorkflowWithFailureHandler` interface

**Files:**
- Create: `src/workflow/workflow-with-failure-handler.ts`
- Modify: `src/index.ts`

- [ ] **Step 1: Create the interface file**

```typescript
// src/workflow/workflow-with-failure-handler.ts
export interface WorkflowWithFailureHandler<TInput = unknown> {
  onFailed(error: Error, input: TInput, jobId: string): Promise<void>;
}
```

- [ ] **Step 2: Export from `src/index.ts`**

Add this line to `src/index.ts` after the existing workflow exports (after line `export { Step } from './decorators/step.decorator';`):

```typescript
export type { WorkflowWithFailureHandler } from './workflow/workflow-with-failure-handler';
```

- [ ] **Step 3: Verify TypeScript compiles**

```bash
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add src/workflow/workflow-with-failure-handler.ts src/index.ts
git commit -m "feat: add WorkflowWithFailureHandler interface"
```

---

## Task 2: Extend result queue data types + fix success payload + existing tests

**Files:**
- Modify: `src/queue/workflow-queue.ts`
- Modify: `src/client/workflow-result-worker.ts`
- Modify: `src/engine/dozer-engine.ts`
- Modify: `src/dozer-engine.spec.ts`

- [ ] **Step 1: Update `WorkflowResultQueueJobData` in `src/queue/workflow-queue.ts`**

Replace:
```typescript
export interface WorkflowResultQueueJobData<TResult = unknown> {
  jobId: string;
  workflowName: string;
  result: TResult;
}
```

With:
```typescript
export interface WorkflowResultQueueJobData<TResult = unknown> {
  jobId: string;
  workflowName: string;
  status: 'completed' | 'failed';
  result: TResult | null;
  error?: string;
}
```

- [ ] **Step 2: Update `WorkflowResultMessage` in `src/client/workflow-result-worker.ts`**

Replace:
```typescript
export interface WorkflowResultMessage<TResult = unknown> {
  resultJobId: string;
  resultJobName: string;
  workflowJobId: string;
  workflowName: string;
  result: TResult;
}
```

With:
```typescript
export interface WorkflowResultMessage<TResult = unknown> {
  resultJobId: string;
  resultJobName: string;
  workflowJobId: string;
  workflowName: string;
  status: 'completed' | 'failed';
  result: TResult | null;
  error?: string;
}
```

- [ ] **Step 3: Update `decodeWorkflowResultJob` in `src/client/workflow-result-worker.ts`**

Replace:
```typescript
return {
  resultJobId,
  resultJobName: job.name,
  workflowJobId: payload.jobId,
  workflowName: payload.workflowName,
  result: deserializeFromStorage(payload.result) as TResult,
};
```

With:
```typescript
return {
  resultJobId,
  resultJobName: job.name,
  workflowJobId: payload.jobId,
  workflowName: payload.workflowName,
  status: (payload.status as 'completed' | 'failed' | undefined) ?? 'completed',
  result: deserializeFromStorage(payload.result) as TResult | null,
  error: payload.error as string | undefined,
};
```

- [ ] **Step 4: Update `enqueueWorkflowResult` in `src/engine/dozer-engine.ts` to set `status: 'completed'` on success**

In `enqueueWorkflowResult`, update the payload construction. Replace:
```typescript
    const payload: WorkflowResultQueueJobData<unknown> = {
      jobId: job.id,
      workflowName: job.name,
      result: await serializeForStorage(
        result,
        'workflow result queue payload',
      ),
    };
```

With:
```typescript
    const payload: WorkflowResultQueueJobData<unknown> = {
      jobId: job.id,
      workflowName: job.name,
      status: 'completed',
      result: await serializeForStorage(
        result,
        'workflow result queue payload',
      ),
    };
```

- [ ] **Step 5: Run tests — expect failures in existing result queue tests**

```bash
npm test 2>&1 | grep -E "FAIL|PASS|●"
```

Expected: some tests fail because existing assertions don't include `status: 'completed'` in `data`, and the `decodeWorkflowResultJob` test doesn't include `status` in raw payload.

- [ ] **Step 6: Fix existing assertion in `dozer-engine.spec.ts` — result queue publish test**

Find the test `'publishes completed workflow result to configured result queue'` (around line 1527). Update the `toMatchObject` assertion to include `status: 'completed'`:

```typescript
      expect(resultQueue.added[0]).toMatchObject({
        name: 'workflow-result',
        data: {
          jobId,
          workflowName: 'result-queue-workflow',
          status: 'completed',
          result: { value: 11 },
        },
        options: {
          jobId,
          removeOnComplete: true,
        },
      });
```

- [ ] **Step 7: Fix existing test `'decodes result queue job payload...'` (around line 2017)**

Update the `rawJob` object to include `status: 'completed'`, and add `status` assertion:

```typescript
    const rawJob = {
      id: '$123',
      name: 'workflow-result',
      data: {
        jobId: '123',
        workflowName: 'my-workflow',
        status: 'completed',
        result: {
          __dozer_serialized__: 'date',
          v: '2026-02-25T00:00:00.000Z',
        },
      },
    } as unknown as Parameters<typeof decodeWorkflowResultJob<Date>>[0];

    const decoded = decodeWorkflowResultJob<Date>(rawJob);
    expect(decoded).toMatchObject({
      resultJobId: '$123',
      resultJobName: 'workflow-result',
      workflowJobId: '123',
      workflowName: 'my-workflow',
      status: 'completed',
    });
    expect(decoded.result).toBeInstanceOf(Date);
```

- [ ] **Step 8: Run tests — all should pass**

```bash
npm test
```

Expected: all tests pass.

- [ ] **Step 9: Commit**

```bash
git add src/queue/workflow-queue.ts src/client/workflow-result-worker.ts src/engine/dozer-engine.ts src/dozer-engine.spec.ts
git commit -m "feat: add status/error fields to result queue payload (breaking)"
```

---

## Task 3: Add config options (type-only changes)

**Files:**
- Modify: `src/decorators/workflow.decorator.ts`
- Modify: `src/dozer.module.ts`

- [ ] **Step 1: Add `publishOnFailure` to `WorkflowResultQueueOptions` in `src/decorators/workflow.decorator.ts`**

Replace:
```typescript
export interface WorkflowResultQueueOptions {
  jobName?: string;
  job?: WorkflowJobOptions;
}
```

With:
```typescript
export interface WorkflowResultQueueOptions {
  jobName?: string;
  job?: WorkflowJobOptions;
  publishOnFailure?: boolean;
}
```

- [ ] **Step 2: Add `onWorkflowFailed` to `DozerModuleOptions` in `src/dozer.module.ts`**

Replace:
```typescript
export interface DozerModuleOptions {
  driver?: WorkflowQueueDriver;
  queue?: BullMQQueueLike<unknown>;
  resultQueue?: BullMQQueueLike<WorkflowResultQueueJobData<unknown>>;
  defaults?: DozerDefaultsOptions;
}
```

With:
```typescript
export interface DozerModuleOptions {
  driver?: WorkflowQueueDriver;
  queue?: BullMQQueueLike<unknown>;
  resultQueue?: BullMQQueueLike<WorkflowResultQueueJobData<unknown>>;
  defaults?: DozerDefaultsOptions;
  onWorkflowFailed?: (
    jobId: string,
    workflowName: string,
    error: Error,
  ) => Promise<void> | void;
}
```

- [ ] **Step 3: Verify TypeScript compiles and tests pass**

```bash
npx tsc --noEmit && npm test
```

Expected: no errors, all tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/decorators/workflow.decorator.ts src/dozer.module.ts
git commit -m "feat: add publishOnFailure and onWorkflowFailed config options"
```

---

## Task 4: TDD — per-workflow `onFailed` method

**Files:**
- Modify: `src/dozer-engine.spec.ts`
- Modify: `src/engine/dozer-engine.ts`

- [ ] **Step 1: Add test fixtures to `src/dozer-engine.spec.ts`**

Add these declarations near the other `@Injectable()` classes (before line 195 where `RecoveryWorkflow` starts):

```typescript
@Injectable()
class OnFailedSpy {
  calls: Array<{ error: Error; input: unknown; jobId: string }> = [];
  throwOnCall = false;
}

@Workflow({ name: 'on-failed-workflow' })
class OnFailedWorkflow {
  constructor(private readonly spy: OnFailedSpy) {}

  @Step({ name: 'fail-step' })
  failStep(): Promise<void> {
    throw new Error('step-on-failed-error');
  }

  async run(input: { value: number }): Promise<void> {
    await this.failStep();
  }

  async onFailed(
    error: Error,
    input: { value: number },
    jobId: string,
  ): Promise<void> {
    if (this.spy.throwOnCall) {
      throw new Error('on-failed-handler-threw');
    }
    this.spy.calls.push({ error, input, jobId });
  }
}

@Workflow({ name: 'on-failed-non-retryable-workflow' })
class OnFailedNonRetryableWorkflow {
  constructor(private readonly spy: OnFailedSpy) {}

  @Step({ name: 'non-retryable-step' })
  failStep(): Promise<void> {
    throw new NonRetryableError('non-retryable-step-error');
  }

  async run(): Promise<void> {
    await this.failStep();
  }

  async onFailed(error: Error, input: unknown, jobId: string): Promise<void> {
    this.spy.calls.push({ error, input, jobId });
  }
}

@Workflow({ name: 'no-on-failed-workflow' })
class NoOnFailedWorkflow {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new Error('no-handler-error');
  }

  async run(): Promise<void> {
    await this.fail();
  }
}
```

- [ ] **Step 2: Add the new test cases to `src/dozer-engine.spec.ts`**

At the end of the `describe('DozerEngine (library unit tests)')` block (before the closing `}`), add:

```typescript
  it('calls onFailed method with error, input, and jobId on terminal failure', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([OnFailedWorkflow], [OnFailedSpy]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedSpy);
      const jobId = await localEngine.start('on-failed-workflow', {
        value: 42,
      });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'step-on-failed-error',
      );

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0].error.message).toBe('step-on-failed-error');
      expect(spy.calls[0].input).toEqual({ value: 42 });
      expect(spy.calls[0].jobId).toBe(jobId);
    } finally {
      await localModule.close();
    }
  });

  it('suppresses errors thrown inside onFailed and still throws original error', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([OnFailedWorkflow], [OnFailedSpy]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedSpy);
      spy.throwOnCall = true;

      const jobId = await localEngine.start('on-failed-workflow', {
        value: 1,
      });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'step-on-failed-error',
      );
    } finally {
      await localModule.close();
    }
  });

  it('does not crash when workflow has no onFailed method', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([NoOnFailedWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('no-on-failed-workflow', {});
      await expect(localEngine.run(jobId)).rejects.toThrow('no-handler-error');
    } finally {
      await localModule.close();
    }
  });

  it('calls onFailed when NonRetryableError is thrown', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature(
          [OnFailedNonRetryableWorkflow],
          [OnFailedSpy],
        ),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedSpy);
      const jobId = await localEngine.start(
        'on-failed-non-retryable-workflow',
        {},
      );

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'non-retryable-step-error',
      );

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0].error.message).toBe('non-retryable-step-error');
    } finally {
      await localModule.close();
    }
  });

```

- [ ] **Step 3: Run tests to verify they fail**

```bash
npx jest --testPathPattern=dozer-engine --verbose 2>&1 | grep -E "✓|✗|×|●|PASS|FAIL|calls onFailed|suppresses errors|does not crash|calls onFailed when Non"
```

Expected: the 4 new tests fail with something like "received function did not throw" or spy.calls is empty.

- [ ] **Step 4: Implement `handleWorkflowFailure` and update `engine.run()` in `src/engine/dozer-engine.ts`**

**4a.** Declare `lastWorkflow` and `lastInput` variables outside the while loop. In `engine.run()`, find the while loop and add before it:

```typescript
    let lastWorkflow: unknown;
    let lastInput: unknown;
```

**4b.** Inside the try block, after `const workflow = this.registry.instantiate(definition);`, add:

```typescript
        lastWorkflow = workflow;
```

And after `const input = deserializeFromStorage(job.data[DOZER_JOB_INPUT_KEY]);`, add:

```typescript
        lastInput = input;
```

So the try block becomes:

```typescript
        const workflow = this.registry.instantiate(definition);
        lastWorkflow = workflow;
        const input = deserializeFromStorage(job.data[DOZER_JOB_INPUT_KEY]);
        lastInput = input;
```

**4c.** In the catch block, find the terminal failure section (after all the `continue` statements):

```typescript
        const stateStore = new WorkflowStateStore(job);
        await stateStore.markFailed(error);
        throw error;
```

Replace with:

```typescript
        const stateStore = new WorkflowStateStore(job);
        await stateStore.markFailed(error);
        if (definition) {
          await this.handleWorkflowFailure(
            job,
            definition,
            lastWorkflow,
            lastInput,
            asThrownError(error),
          );
        }
        throw error;
```

**4d.** Add the `handleWorkflowFailure` private method to `DozerEngine`. Add it after `runDeterminismProbe` (before `async run(`):

```typescript
  private async handleWorkflowFailure(
    job: WorkflowJob<unknown>,
    definition: RegisteredWorkflow,
    workflow: unknown,
    input: unknown,
    error: Error,
  ): Promise<void> {
    const onFailed = (workflow as Record<string, unknown> | undefined)
      ?.onFailed;
    if (typeof onFailed === 'function') {
      try {
        await (
          onFailed as (
            e: Error,
            i: unknown,
            id: string,
          ) => Promise<void>
        ).call(workflow, error, input, job.id);
      } catch {
        // suppressed
      }
    }

    const globalCallback = this.moduleOptions.onWorkflowFailed;
    if (globalCallback) {
      try {
        await globalCallback(job.id, job.name, error);
      } catch {
        // suppressed
      }
    }

    const shouldPublishFailure =
      definition.options.resultQueue?.publishOnFailure === true &&
      Boolean(this.moduleOptions.resultQueue);
    if (shouldPublishFailure) {
      try {
        await this.enqueueWorkflowResult(job, definition, null, error);
      } catch {
        // suppressed
      }
    }
  }
```

**4e.** Update `enqueueWorkflowResult` signature to accept optional `failureError` and handle it. Replace the existing `enqueueWorkflowResult` method:

```typescript
  private async enqueueWorkflowResult(
    job: WorkflowJob<unknown>,
    definition: RegisteredWorkflow,
    result: unknown,
    failureError?: Error,
  ): Promise<void> {
    const resultQueueOptions = definition.options.resultQueue;
    const resultQueue = this.moduleOptions.resultQueue;
    if (!resultQueueOptions || !resultQueue) {
      return;
    }

    const isFailure = failureError !== undefined;
    const payload: WorkflowResultQueueJobData<unknown> = {
      jobId: job.id,
      workflowName: job.name,
      status: isFailure ? 'failed' : 'completed',
      result: isFailure
        ? null
        : await serializeForStorage(result, 'workflow result queue payload'),
      ...(isFailure ? { error: failureError.message } : {}),
    };
    const resultJobName = resultQueueOptions.jobName ?? `${job.name}:result`;
    const resultQueueJobId = toWorkflowResultQueueJobId(job.id);
    const resultJobOptions: WorkflowJobOptions = {
      ...(resultQueueOptions.job ?? {}),
      jobId: resultQueueJobId,
    };

    try {
      await resultQueue.add(resultJobName, payload, resultJobOptions);
      return;
    } catch (error) {
      const resultJobId = resultQueueJobId;
      if (
        isDuplicateJobIdError(error) &&
        (await resultQueue.getJob(resultJobId))
      ) {
        return;
      }

      throw new WorkflowResultPublishStageError(error);
    }
  }
```

- [ ] **Step 5: Run the new tests — they should pass**

```bash
npx jest --testPathPattern=dozer-engine --verbose 2>&1 | grep -E "✓|✗|×|●|PASS|FAIL|calls onFailed|suppresses errors|does not crash|calls onFailed when Non"
```

Expected: all 4 new tests pass.

- [ ] **Step 6: Run full test suite — all should pass**

```bash
npm test
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/engine/dozer-engine.ts src/dozer-engine.spec.ts
git commit -m "feat: call workflow onFailed method on terminal failure"
```

---

## Task 5: TDD — global `onWorkflowFailed` callback

**Files:**
- Modify: `src/dozer-engine.spec.ts`

(Implementation is already in `handleWorkflowFailure` from Task 4. Only tests needed.)

- [ ] **Step 1: Add test fixtures to `src/dozer-engine.spec.ts`**

Add near the other `@Workflow` fixture declarations:

```typescript
@Workflow({ name: 'global-callback-workflow' })
class GlobalCallbackWorkflow {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new Error('global-callback-error');
  }

  async run(): Promise<void> {
    await this.fail();
  }
}

@Workflow({ name: 'global-callback-non-retryable-workflow' })
class GlobalCallbackNonRetryableWorkflow {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new NonRetryableError('global-nr-error');
  }

  async run(): Promise<void> {
    await this.fail();
  }
}
```

- [ ] **Step 2: Add test cases to `src/dozer-engine.spec.ts`**

At the end of `describe('DozerEngine (library unit tests)')`:

```typescript
  it('calls global onWorkflowFailed callback on terminal failure', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const callbackCalls: Array<{
      jobId: string;
      workflowName: string;
      error: Error;
    }> = [];
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          onWorkflowFailed: (jobId, workflowName, error) => {
            callbackCalls.push({ jobId, workflowName, error });
          },
        }),
        DozerModule.forFeature([GlobalCallbackWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('global-callback-workflow', {});

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'global-callback-error',
      );

      expect(callbackCalls).toHaveLength(1);
      expect(callbackCalls[0].jobId).toBe(jobId);
      expect(callbackCalls[0].workflowName).toBe('global-callback-workflow');
      expect(callbackCalls[0].error.message).toBe('global-callback-error');
    } finally {
      await localModule.close();
    }
  });

  it('suppresses errors thrown inside global onWorkflowFailed callback', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          onWorkflowFailed: () => {
            throw new Error('callback-threw');
          },
        }),
        DozerModule.forFeature([GlobalCallbackWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('global-callback-workflow', {});

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'global-callback-error',
      );
    } finally {
      await localModule.close();
    }
  });

  it('calls global onWorkflowFailed when NonRetryableError is thrown', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const callbackCalls: string[] = [];
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          onWorkflowFailed: (_jobId, _name, error) => {
            callbackCalls.push(error.message);
          },
        }),
        DozerModule.forFeature([GlobalCallbackNonRetryableWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start(
        'global-callback-non-retryable-workflow',
        {},
      );

      await expect(localEngine.run(jobId)).rejects.toThrow('global-nr-error');

      expect(callbackCalls).toEqual(['global-nr-error']);
    } finally {
      await localModule.close();
    }
  });
```

- [ ] **Step 3: Run the new tests**

```bash
npx jest --testPathPattern=dozer-engine --verbose 2>&1 | grep -E "✓|✗|×|●|PASS|FAIL|global onWorkflow|suppresses errors.*global|calls global.*Non"
```

Expected: all 3 new tests pass (implementation already exists from Task 4).

- [ ] **Step 4: Run full test suite**

```bash
npm test
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/dozer-engine.spec.ts
git commit -m "test: add global onWorkflowFailed callback tests"
```

---

## Task 6: TDD — `publishOnFailure` result queue publishing

**Files:**
- Modify: `src/dozer-engine.spec.ts`

(Implementation already present in `handleWorkflowFailure` from Task 4.)

- [ ] **Step 1: Add workflow fixtures to `src/dozer-engine.spec.ts`**

```typescript
@Workflow({
  name: 'failure-publish-workflow',
  resultQueue: {
    jobName: 'workflow-result',
    publishOnFailure: true,
  },
})
class FailurePublishWorkflow {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new Error('failure-publish-error');
  }

  async run(input: { value: number }): Promise<void> {
    await this.fail();
  }
}

@Workflow({
  name: 'failure-no-publish-workflow',
  resultQueue: {
    jobName: 'workflow-result',
    // publishOnFailure not set — defaults to false
  },
})
class FailureNoPublishWorkflow {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new Error('no-publish-failure-error');
  }

  async run(): Promise<void> {
    await this.fail();
  }
}
```

- [ ] **Step 2: Add test cases**

At the end of `describe('DozerEngine (library unit tests)')`:

```typescript
  it('publishes failure payload to result queue when publishOnFailure is true', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const resultQueue = new CapturingResultQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue, resultQueue }),
        DozerModule.forFeature([FailurePublishWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('failure-publish-workflow', {
        value: 7,
      });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'failure-publish-error',
      );

      expect(resultQueue.added).toHaveLength(1);
      expect(resultQueue.added[0]).toMatchObject({
        name: 'workflow-result',
        data: {
          jobId,
          workflowName: 'failure-publish-workflow',
          status: 'failed',
          result: null,
          error: 'failure-publish-error',
        },
      });
    } finally {
      await localModule.close();
    }
  });

  it('does not publish to result queue on failure when publishOnFailure is false', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const resultQueue = new CapturingResultQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue, resultQueue }),
        DozerModule.forFeature([FailureNoPublishWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start(
        'failure-no-publish-workflow',
        {},
      );

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'no-publish-failure-error',
      );

      expect(resultQueue.added).toHaveLength(0);
    } finally {
      await localModule.close();
    }
  });
```

- [ ] **Step 3: Run the new tests**

```bash
npx jest --testPathPattern=dozer-engine --verbose 2>&1 | grep -E "✓|✗|×|●|PASS|FAIL|publishOnFailure|does not publish"
```

Expected: both tests pass.

- [ ] **Step 4: Run full test suite**

```bash
npm test
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/dozer-engine.spec.ts
git commit -m "test: add publishOnFailure result queue tests"
```

---

## Task 7: Version bump to 0.6.0 + final verification

**Files:**
- Modify: `package.json`

- [ ] **Step 1: Bump version in `package.json`**

Change line `"version": "0.5.0"` to `"version": "0.6.0"`.

- [ ] **Step 2: Run full test suite one final time**

```bash
npm test
```

Expected: all tests pass.

- [ ] **Step 3: Verify TypeScript build**

```bash
npm run build
```

Expected: build succeeds with no errors.

- [ ] **Step 4: Commit**

```bash
git add package.json
git commit -m "chore: bump version to 0.6.0"
```
