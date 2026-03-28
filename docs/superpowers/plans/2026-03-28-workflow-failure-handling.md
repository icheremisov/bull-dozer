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

---

## Task 8: Split `src/dozer-engine.spec.ts` into focused spec files

**Goal:** `src/dozer-engine.spec.ts` is 2473 lines and mixes tests for core mechanics, retries, serialization, result queue, determinism, failure handling, module registration, and client — all in one monolithic file. Split into focused files so each file covers one concern.

**Files:**
- Create: `src/test/workflow-test-utils.ts` — shared test infrastructure used by multiple spec files
- Modify: `src/dozer-engine.spec.ts` — keep only core mechanics tests
- Create: `src/dozer-engine-retries.spec.ts` — step/workflow retry behavior
- Create: `src/dozer-engine-serialization.spec.ts` — input/output serialization
- Create: `src/dozer-engine-result-queue.spec.ts` — result queue success path
- Create: `src/dozer-engine-determinism.spec.ts` — non-determinism detection and determinism probe
- Create: `src/dozer-engine-failure.spec.ts` — failure handling (onFailed, global callback, publishOnFailure)
- Create: `src/dozer-module.spec.ts` — module registration constraint tests
- Create: `src/dozer-client.spec.ts` — DozerClient module tests

**Before you begin:** Read `src/dozer-engine.spec.ts` in full to understand what's there. The file starts with imports (lines 1-25), shared helpers (lines 27-93), shared test infrastructure classes (lines 94-198), fixture classes (lines 200-1041), then three `describe` blocks: `'DozerEngine (library unit tests)'` (line 1042), `'DozerModule registration constraints'` (line 2250), `'DozerClient module'` (line 2270).

---

- [ ] **Step 1: Create `src/test/workflow-test-utils.ts`**

This file holds test infrastructure shared by multiple spec files. Copy the following from `src/dozer-engine.spec.ts` exactly (do not change the code):

- `sleep` function (currently lines 27-29)
- `CapturingResultQueue` class (lines 94-131)
- `FailOnceResultQueue` class (lines 133-148)
- `DuplicateJobIdResultQueue` class (lines 150-198)
- `FailOnceService` class (lines 46-63) — used in both core and retry spec files

The file needs these imports:

```typescript
import { Injectable } from '@nestjs/common';
import {
  BullMQQueueLike,
  WorkflowJobOptions,
  WorkflowResultQueueJobData,
} from '../index';
```

Export all five items with `export`.

- [ ] **Step 2: Verify the shared utils file compiles**

```bash
npx tsc --noEmit
```

Expected: no errors.

---

- [ ] **Step 3: Create `src/dozer-engine-failure.spec.ts`**

This file tests failure callbacks and failure result queue publishing. All 9 tests in this group already use `localModule` pattern (no shared `beforeEach` needed).

**Tests to include** (exact `it()` descriptions):
- `'calls onFailed method with error, input, and jobId on terminal failure'`
- `'suppresses errors thrown inside onFailed and still throws original error'`
- `'does not crash when workflow has no onFailed method'`
- `'calls onFailed when NonRetryableError is thrown'`
- `'calls global onWorkflowFailed callback on terminal failure'`
- `'suppresses errors thrown inside global onWorkflowFailed callback'`
- `'calls global onWorkflowFailed when NonRetryableError is thrown'`
- `'publishes failure payload to result queue when publishOnFailure is true'`
- `'does not publish to result queue on failure when publishOnFailure is false'`

**Fixture classes to copy** from `src/dozer-engine.spec.ts`:
- `OnFailedSpy` (lines 200-205)
- `OnFailedWorkflow` (lines 206-230)
- `OnFailedNonRetryableWorkflow` (lines 231-248)
- `NoOnFailedWorkflow` (lines 249-260)
- `GlobalCallbackWorkflow` (lines 261-272)
- `GlobalCallbackNonRetryableWorkflow` (lines 273-284)
- `FailurePublishWorkflow` (lines ~1003-1018, the `@Workflow({ name: 'failure-publish-workflow'...})` class)
- `FailureNoPublishWorkflow` (lines ~1020-1041, the `@Workflow({ name: 'failure-no-publish-workflow'...})` class)

**Imports needed:**

```typescript
import { Injectable } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import {
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  NonRetryableError,
  Step,
  Workflow,
} from './index';
import { CapturingResultQueue } from './test/workflow-test-utils';
```

Wrap all tests in `describe('DozerEngine failure handling', () => { ... })`.

- [ ] **Step 4: Run failure spec to confirm it passes**

```bash
npx jest --testPathPattern=dozer-engine-failure --verbose
```

Expected: 9 tests pass.

---

- [ ] **Step 5: Create `src/dozer-engine-result-queue.spec.ts`**

This file tests result queue publishing on success (the success path). All 3 tests create their own `localModule`.

**Tests to include:**
- `'publishes completed workflow result to configured result queue'`
- `'keeps workflow in completing status when result queue publish fails and resumes finalize later'`
- `'resumes completing workflow when result job already exists without creating duplicate'`

**Fixture classes to copy:**
- `ResultQueueWorkflow` (lines ~778-783, the `@Workflow({ name: 'result-queue-workflow', resultQueue: { jobName: 'workflow-result' } })` class)

**Imports needed:**

```typescript
import { Test } from '@nestjs/testing';
import {
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  Step,
  toWorkflowResultQueueJobId,
  Workflow,
  WorkflowJobOptions,
} from './index';
import {
  CapturingResultQueue,
  DuplicateJobIdResultQueue,
  FailOnceResultQueue,
} from './test/workflow-test-utils';
```

Wrap in `describe('DozerEngine result queue', () => { ... })`.

- [ ] **Step 6: Run result queue spec**

```bash
npx jest --testPathPattern=dozer-engine-result-queue --verbose
```

Expected: 3 tests pass.

---

- [ ] **Step 7: Create `src/dozer-engine-determinism.spec.ts`**

Tests for non-determinism detection and the determinism probe feature.

**Tests to include:**
- `'detects non-deterministic replay'`
- `'replays cached nested steps without trace conflicts'`
- `'runs determinism probe after completion and reuses cached step results'`
- `'fails determinism probe when replayed result diverges'`
- `'fails determinism probe when replay run is too slow'`
- `'supports module-level defaults for worker determinism probe'`

The first 5 tests use the shared `moduleRef`/`engine`. The 6th (`'supports module-level defaults...'`) uses a `localModule`.

**Fixture classes to copy:**
- `NestedReplayStats` (lines 70-76)
- `DeterminismProbeStats` (lines 77-81)
- `NonDeterministicWorkflow` (lines 405-441, the `@Workflow({ name: 'nondeterministic-workflow' })` class)
- `NestedReplayWorkflow` (lines 628-663)
- `DeterminismProbeStableWorkflow` (lines ~784-804)
- `DeterminismProbeRandomWorkflow` (lines ~805-817)
- `DeterminismProbeSlowWorkflow` (lines ~818-831)
- `GlobalDeterminismProbeRandomWorkflow` (lines ~832-838)

**Module setup** — add `beforeEach`/`afterEach` that registers a `DozerModule` with only these workflows:

```typescript
let moduleRef: TestingModule;
let queue: InMemoryWorkflowQueue;
let engine: DozerEngine;

beforeEach(async () => {
  queue = new InMemoryWorkflowQueue();
  moduleRef = await Test.createTestingModule({
    imports: [
      DozerModule.forRoot({ driver: queue }),
      DozerModule.forFeature(
        [
          NonDeterministicWorkflow,
          NestedReplayWorkflow,
          DeterminismProbeStableWorkflow,
          DeterminismProbeRandomWorkflow,
          DeterminismProbeSlowWorkflow,
          GlobalDeterminismProbeRandomWorkflow,
        ],
        [NestedReplayStats, DeterminismProbeStats],
      ),
    ],
  }).compile();
  await moduleRef.init();
  engine = moduleRef.get(DozerEngine);
});

afterEach(async () => {
  if (moduleRef) await moduleRef.close();
});
```

**Imports needed:**

```typescript
import { Test, TestingModule } from '@nestjs/testing';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  NonDeterminismError,
  Step,
  StepReplayConflictError,
  Workflow,
  WORKFLOW_STATUS,
} from './index';
import { sleep } from './test/workflow-test-utils';
```

Wrap in `describe('DozerEngine determinism', () => { ... })`.

- [ ] **Step 8: Run determinism spec**

```bash
npx jest --testPathPattern=dozer-engine-determinism --verbose
```

Expected: 6 tests pass.

---

- [ ] **Step 9: Create `src/dozer-engine-serialization.spec.ts`**

Tests for binary, typed-array, Date, and non-serializable input/output handling.

**Tests to include:**
- `'restores binary and byte-array workflow inputs on replay'`
- `'restores typed-array step results on replay'`
- `'rejects non-serializable workflow input values'`
- `'fails workflow when step result is non-serializable'`
- `'serializes and restores Date in workflow input'`
- `'serializes and restores Date step results on replay'`

All 6 tests use the shared `moduleRef`/`engine`.

**Fixture classes to copy:**
- `BinaryStats` (lines 64-69)
- `BinaryInputWorkflow` (lines 442-502)
- `TypedArrayResultWorkflow` (lines 503-543)
- `NonSerializableStepResultWorkflow` (lines 544-557)
- `DatePayloadWorkflow` (lines 558-591)
- `DateStepResultWorkflow` (lines 592-627)

**Module setup** — `beforeEach` with just these workflows:

```typescript
beforeEach(async () => {
  queue = new InMemoryWorkflowQueue();
  moduleRef = await Test.createTestingModule({
    imports: [
      DozerModule.forRoot({ driver: queue }),
      DozerModule.forFeature(
        [
          BinaryInputWorkflow,
          TypedArrayResultWorkflow,
          NonSerializableStepResultWorkflow,
          DatePayloadWorkflow,
          DateStepResultWorkflow,
        ],
        [BinaryStats],
      ),
    ],
  }).compile();
  await moduleRef.init();
  engine = moduleRef.get(DozerEngine);
});
```

**Imports needed:**

```typescript
import { Test, TestingModule } from '@nestjs/testing';
import {
  DOZER_JOB_INPUT_KEY,
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  SerializationError,
  Step,
  Workflow,
  WORKFLOW_STATUS,
} from './index';
```

Wrap in `describe('DozerEngine serialization', () => { ... })`.

- [ ] **Step 10: Run serialization spec**

```bash
npx jest --testPathPattern=dozer-engine-serialization --verbose
```

Expected: 6 tests pass.

---

- [ ] **Step 11: Create `src/dozer-engine-retries.spec.ts`**

Tests for step retries, `NonRetryableError`, workflow-level retry, module/workflow defaults, timeout, and job options.

**Tests to include:**
- `'retries unstable steps by retry policy'`
- `'does not retry step when NonRetryableError is thrown'`
- `'supports timeout handling with compensating actions in workflow run'`
- `'automatically resumes workflow by workflowRetry settings'`
- `'applies workflow retry backoff strategy delays'`
- `'applies workflow-level default retry options for steps without own retry'`
- `'restarts whole workflow on step retry using a fresh workflow instance'`
- `'applies module-level default retry options for steps'`
- `'lets step-level retry options override module defaults'`
- `'applies module-level default workflowRetry options'`
- `'lets workflow-level workflowRetry options override module defaults'`
- `'merges global and workflow job options when creating jobs'`

The first 7 tests use the shared `moduleRef`/`engine`. The last 5 tests use `localModule`.

**Fixture classes to copy:**
- `FailOnceService` — import from `./test/workflow-test-utils` (do not duplicate)
- `TimeoutCompensationStats` (lines 82-87)
- `WorkflowAutoResumeStats` (lines 88-93)
- `RetryWorkflow` (lines 321-338)
- `NonRetryableStepWorkflow` (the `@Workflow({ name: 'non-retryable-step-workflow' })` class at lines ~876-895)
- `TimeoutCompensationWorkflow` (lines ~896-940)
- `WorkflowAutoResumeWorkflow` (lines ~941-986)
- `WorkflowRetryLinearWorkflow` (lines ~987-1001)
- `WorkflowDefaultRetryWorkflow` (lines ~664-688, the `@Workflow({ name: 'workflow-default-retry-workflow' })`)
- `GlobalDefaultRetryWorkflow` (lines ~689-706)
- `GlobalDefaultRetryOverrideWorkflow` (lines ~707-729)
- `RetryRestartsWholeFlowWorkflow` (lines ~730-755)
- `JobOptionsWorkflow` (lines ~756-768)
- `GlobalWorkflowRetryWorkflow` (lines ~839-851)
- `GlobalWorkflowRetryOverrideWorkflow` (lines ~852-875)

**Module setup** — `beforeEach` with just the retry-related workflows (for the first 7 tests):

```typescript
beforeEach(async () => {
  queue = new InMemoryWorkflowQueue();
  moduleRef = await Test.createTestingModule({
    imports: [
      DozerModule.forRoot({ driver: queue }),
      DozerModule.forFeature(
        [
          RetryWorkflow,
          NonRetryableStepWorkflow,
          TimeoutCompensationWorkflow,
          WorkflowAutoResumeWorkflow,
          WorkflowRetryLinearWorkflow,
          WorkflowDefaultRetryWorkflow,
          RetryRestartsWholeFlowWorkflow,
        ],
        [FailOnceService, TimeoutCompensationStats, WorkflowAutoResumeStats],
      ),
    ],
  }).compile();
  await moduleRef.init();
  engine = moduleRef.get(DozerEngine);
});
```

**Imports needed:**

```typescript
import { Test, TestingModule } from '@nestjs/testing';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  NonRetryableError,
  Step,
  TimeoutError,
  Workflow,
  WORKFLOW_STATUS,
  WorkflowJobOptions,
} from './index';
import { FailOnceService, sleep } from './test/workflow-test-utils';
```

Wrap in `describe('DozerEngine retries', () => { ... })`.

- [ ] **Step 12: Run retries spec**

```bash
npx jest --testPathPattern=dozer-engine-retries --verbose
```

Expected: 12 tests pass.

---

- [ ] **Step 13: Create `src/dozer-module.spec.ts`**

Copy the entire `describe('DozerModule registration constraints', () => { ... })` block from `src/dozer-engine.spec.ts` (lines ~2250-2268) along with its fixtures:

- `DuplicateNameWorkflowA` and `DuplicateNameWorkflowB` classes (in the fixture section — search for `duplicate-workflow-name`)

**Imports needed:**

```typescript
import { Test } from '@nestjs/testing';
import { DozerModule, InMemoryWorkflowQueue, Workflow } from './index';
```

Keep the same `describe` name: `'DozerModule registration constraints'`.

- [ ] **Step 14: Run module spec**

```bash
npx jest --testPathPattern=dozer-module --verbose
```

Expected: 1 test passes.

---

- [ ] **Step 15: Create `src/dozer-client.spec.ts`**

Copy the entire `describe('DozerClient module', () => { ... })` block from `src/dozer-engine.spec.ts` (lines ~2270-2472) along with any workflow fixtures it uses.

Read the test bodies to identify which workflow fixture classes they reference (look for `DozerModule.forFeature([...])` calls inside the test bodies). Copy only the fixtures actually referenced. The `ResultQueueWorkflow` fixture (line ~778) is likely needed — check and include it if so.

**Imports needed** — determine from the test bodies. Will include at minimum:

```typescript
import { Test } from '@nestjs/testing';
import {
  createWorkflowResultProcessor,
  decodeWorkflowResultJob,
  DozerClient,
  DozerModule,
  InMemoryWorkflowQueue,
  Step,
  toWorkflowResultQueueJobId,
  Workflow,
  WORKFLOW_STATUS,
} from './index';
import { CapturingResultQueue } from './test/workflow-test-utils';
```

Keep the same `describe` name: `'DozerClient module'`.

- [ ] **Step 16: Run client spec**

```bash
npx jest --testPathPattern=dozer-client --verbose
```

Expected: 5 tests pass.

---

- [ ] **Step 17: Reduce `src/dozer-engine.spec.ts` to core mechanics**

After all other spec files are working, remove from `src/dozer-engine.spec.ts`:
1. All fixture classes that have been moved to other files or `workflow-test-utils.ts`
2. All `it()` test cases that were moved to other files
3. The `DozerModule registration constraints` describe block (moved to `dozer-module.spec.ts`)
4. The `DozerClient module` describe block (moved to `dozer-client.spec.ts`)
5. The local `sleep` definition (now imported from `./test/workflow-test-utils`)
6. The `CapturingResultQueue`, `FailOnceResultQueue`, `DuplicateJobIdResultQueue` class definitions (moved to `workflow-test-utils.ts`)
7. The `FailOnceService` class definition (moved to `workflow-test-utils.ts`)

**Tests to keep in `dozer-engine.spec.ts`:**
- `'restores workflow state and replays completed steps only once'`
- `'returns workflow job info with status and result by jobId'`
- `'cancels pending workflow job and prevents running it'`
- `'marks state as failed when workflow is not registered'`
- `'supports steps that return void and undefined'`
- `'handles repeated calls of the same step method as separate step keys'`
- `'supports workflows with different input data types'`

**Fixture classes to keep** (those used by the above tests):
- `RecoveryStats`, `BranchService` (injectable services for RecoveryWorkflow)
- `RecoveryWorkflow`, `RetryWorkflow`, `TypedStepWorkflow`, `RepeatedStepWorkflow`, `TypedInputWorkflow`

**Update the `beforeEach` module** to register only these 5 workflows and 3 providers:

```typescript
DozerModule.forFeature(
  [RecoveryWorkflow, RetryWorkflow, TypedStepWorkflow, RepeatedStepWorkflow, TypedInputWorkflow],
  [RecoveryStats, BranchService, FailOnceService],
)
```

**Add import** of `FailOnceService` from `./test/workflow-test-utils` and `sleep` from the same.

Wrap in `describe('DozerEngine core', () => { ... })` (rename the describe to reflect reduced scope).

- [ ] **Step 18: Run the reduced core spec**

```bash
npx jest --testPathPattern=src/dozer-engine.spec --verbose
```

Expected: 7 tests pass.

---

- [ ] **Step 19: Run full test suite**

```bash
npm test
```

Expected: all tests pass — same count as before the split. Verify with:

```bash
npm test 2>&1 | tail -5
```

- [ ] **Step 20: Verify TypeScript**

```bash
npx tsc --noEmit
```

Expected: no errors.

- [ ] **Step 21: Commit**

```bash
git add src/
git commit -m "refactor: split dozer-engine.spec.ts into focused spec files"
```
