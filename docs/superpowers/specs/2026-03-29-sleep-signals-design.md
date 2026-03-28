# Design: Durable Sleep & Signals for dozer

**Date:** 2026-03-29
**Status:** Approved
**Scope:** Non-blocking workflow waiting primitives — `sleep`, `sleepUntil`, `waitForSignal`

---

## Problem

Workflows with polling or external-event steps (e.g. `checkAndProcessVideo`) currently use `while (true) + setTimeout` inside a `@Step`. This blocks the BullMQ worker thread for the entire wait duration — a worker slot is consumed doing nothing. At scale, this degrades throughput significantly.

**Goal:** free the worker immediately when a workflow needs to wait, re-schedule the job via BullMQ's delayed queue, and resume from the exact pause point via the existing replay mechanism.

---

## Primitives (Variant B)

| Primitive | Model | Description |
|---|---|---|
| `this.sleep(ms)` | pull / timer | pause N ms, worker freed |
| `this.sleepUntil(ts)` | pull / timer | pause until UNIX timestamp |
| `this.waitForSignal(name, opts?)` | push / event | pause until external signal |
| `dozerClient.sendSignal(jobId, name, payload?)` | external | deliver signal, wake job |

Signals are fire-and-forget (return `boolean` — delivered or no pending wait). They return `null` on timeout if `timeoutMs` was specified, otherwise wait indefinitely (up to a configurable module-level default).

---

## Base Class: DozerWorkflow

All workflow classes **must** extend `DozerWorkflow`. This is a **hard break** — existing workflows without inheritance will throw at application startup.

```ts
export abstract class DozerWorkflow<TInput = unknown> {
  abstract run(input: TInput): Promise<unknown>;

  protected async sleep(durationMs: number): Promise<void>
  protected async sleepUntil(timestamp: number): Promise<void>
  protected async waitForSignal<T>(
    signalName: string,
    opts?: { timeoutMs?: number },
  ): Promise<T | null>
}
```

Methods are `protected` — only callable from within the workflow subclass.

### @NoStep decorator

Every method on a workflow class (except `constructor` and `run`) must be decorated with either `@Step` or `@NoStep`. `@Workflow` enforces this at decoration time — missing annotation is a startup error.

```ts
@NoStep()
private async downloadProcessAndUpload(...) { /* helper, not a step */ }
```

`@NoStep` stores metadata marker `dozer:nostep` on the method. No runtime behavior changes.

### @Workflow validation additions

1. **Inheritance check**: `WorkflowClass.prototype instanceof DozerWorkflow` — throws if not.
2. **Method annotation check**: iterates `Object.getOwnPropertyNames(WorkflowClass.prototype)`, skips `constructor` and `run`, verifies each method has `STEP_OPTIONS_METADATA` or `NOSTEP_METADATA` — throws with method name if missing.

---

## How Sleep Works

`sleep` is modelled as a **virtual step** that participates in the trace and step cache, identical in mechanics to `@Step`-decorated methods.

### First call (no prior sleep at this trace position)

1. `context.sleep(durationMs)` calls `enterStep('__sleep__')`
2. `hasCachedResult === false`, no `sl[key]` in state
3. Computes `wakeUpAt = Date.now() + durationMs`
4. Saves `state.sl[stepKey] = wakeUpAt`, flushes to BullMQ job data
5. Throws `WorkflowSleepRequestedError(wakeUpAt)`
6. Engine catches it, calls `queue.moveToDelayed(jobId, wakeUpAt, token)`
7. Returns — worker is freed

### Job wakes up (BullMQ fires delayed job)

1. Engine calls `workflow.run(input)` — full replay begins
2. All prior completed steps return instantly from cache (`c` / `u`)
3. Reaches `sleep()` — `enterStep('__sleep__')` → `hasCachedResult === false`
4. Checks `sl[key]`: exists, `Date.now() >= wakeUpAt`
5. Calls `stateStore.completeSleep(key)` — moves key to `u`, deletes from `sl`, flushes
6. Returns `undefined` — workflow continues

### Edge case: job replayed before wakeUpAt

`Date.now() < wakeUpAt` → throws `WorkflowSleepRequestedError(wakeUpAt)` again → engine re-parks with the same timestamp.

### Replay after sleep (subsequent steps)

`enterStep('__sleep__')` → `hasCachedResult === true` (key is in `u`) → returns immediately. Workflow continues without any sleep logic.

---

## How Signals Work

### First call to waitForSignal

1. `context.waitForSignal('payment', { timeoutMs: 3600_000 })`
2. `enterStep('__signal__:payment')` → `hasCachedResult === false`
3. No `ps['payment']` in state
4. Computes `expiresAt = Date.now() + timeoutMs` (or `undefined` if no timeout)
5. Saves `state.ps['payment'] = { k: stepKey, e: expiresAt }`, flushes
6. Throws `WorkflowSignalWaitRequestedError('payment', expiresAt)`
7. Engine: `deadline = expiresAt ?? Date.now() + DEFAULT_SIGNAL_TIMEOUT_MS`
8. Calls `queue.moveToDelayed(jobId, deadline, token)` — worker freed

### Signal delivered (sendSignal)

```ts
await dozerClient.sendSignal(jobId, 'payment', { amount: 100 });
```

1. Gets job from queue, reads state
2. `stateStore.deliverSignal('payment', payload)`:
   - Finds `ps['payment']` → `stepKey`
   - Saves `c[stepKey] = serialize(payload)`
   - Deletes `ps['payment']`
   - Flushes
3. `queue.promoteDelayed(jobId)` — job moves to front of queue immediately
4. Returns `true`

Returns `false` (no error) if no pending signal with that name exists.

### Job wakes up after signal

1. Full replay — all prior steps from cache
2. `waitForSignal('payment')` → `enterStep(...)` → `hasCachedResult === true` (payload in `c`)
3. Returns payload — workflow continues

### Timeout path

1. BullMQ fires the delayed job at `expiresAt`
2. Replay reaches `waitForSignal`
3. `enterStep` → `hasCachedResult === false`
4. `ps['payment']` exists, `Date.now() >= expiresAt`
5. Saves `c[stepKey] = serialize(null)`, deletes `ps['payment']`, flushes
6. Returns `null` — workflow handles timeout

---

## State Model Changes

```ts
export interface CompactWorkflowState {
  s: WorkflowStatusCode;
  c: Record<string, unknown>;
  a?: Record<string, number>;
  u?: Record<string, 1>;
  t: string[];
  r?: unknown;
  e?: string;
  // NEW:
  sl?: Record<string, number>;                         // pending sleeps: stepKey → wakeUpAt
  ps?: Record<string, { k: string; e?: number }>;     // pending signals: signalName → { stepKey, expiresAt? }
}
```

Fields are optional and absent when unused, keeping state compact.

---

## Queue Driver Changes

```ts
export interface WorkflowQueueDriver {
  add(...): Promise<WorkflowJob>;
  get(...): Promise<WorkflowJob | null>;
  // NEW:
  moveToDelayed(jobId: string, timestamp: number, token?: string): Promise<void>;
  promoteDelayed(jobId: string): Promise<void>;
}
```

### BullMQWorkflowQueue

- `moveToDelayed`: `queue.getJob(id)` → `job.moveToDelayed(timestamp, token)`; engine throws `DelayedError` from bullmq after calling this
- `promoteDelayed`: `queue.getJob(id)` → `job.promote()`

### InMemoryWorkflowQueue (tests)

- `moveToDelayed`: stores `{ jobId, timestamp }` in an internal map; `promoteDelayed` clears the entry
- No real timers — delayed jobs stay delayed until explicitly promoted or the test advances time

---

## Engine Changes

```ts
async run(jobId: string, token?: string): Promise<unknown>
```

New catch branches inside the `while (true)` loop:

```ts
if (error instanceof WorkflowSleepRequestedError) {
  await this.queue.moveToDelayed(jobId, error.wakeUpAt, token);
  throw new DelayedError(); // BullMQ: marks job as intentionally delayed, not failed
}

if (error instanceof WorkflowSignalWaitRequestedError) {
  const deadline = error.expiresAt ?? Date.now() + this.resolveDefaultSignalTimeoutMs();
  await this.queue.moveToDelayed(jobId, deadline, token);
  throw new DelayedError();
}
```

`resolveDefaultSignalTimeoutMs()` reads from `moduleOptions.defaults?.signalTimeoutMs` (new optional field, default: 7 days).

---

## DozerClient.sendSignal

```ts
async sendSignal<TPayload = unknown>(
  jobId: string,
  signalName: string,
  payload?: TPayload,
): Promise<boolean>
```

Returns `true` if signal was delivered and job promoted, `false` if no pending signal matched.

---

## DozerModuleOptions additions

```ts
export interface DozerDefaultsOptions {
  job?: WorkflowJobOptions;
  execution?: WorkflowExecutionOptions;
  signalTimeoutMs?: number; // NEW: default max wait for signals without explicit timeout (default: 7 days)
}
```

---

## New Files

| File | Purpose |
|---|---|
| `src/workflow/dozer-workflow.ts` | `DozerWorkflow` abstract base class |
| `src/decorators/no-step.decorator.ts` | `@NoStep` decorator |
| `src/errors/workflow-sleep-requested.error.ts` | Internal error thrown on sleep |
| `src/errors/workflow-signal-wait-requested.error.ts` | Internal error thrown on signal wait |

## Modified Files

| File | Change |
|---|---|
| `src/decorators/workflow.decorator.ts` | Add DozerWorkflow + method annotation validation |
| `src/queue/workflow-queue.ts` | `CompactWorkflowState` sl/ps fields; new queue driver methods |
| `src/queue/bullmq-workflow-queue.ts` | Implement `moveToDelayed`, `promoteDelayed` |
| `src/queue/in-memory-workflow-queue.ts` | Implement `moveToDelayed`, `promoteDelayed` |
| `src/runtime/workflow-state.store.ts` | Sleep and signal state methods |
| `src/runtime/workflow-execution-context.ts` | `sleep()`, `sleepUntil()`, `waitForSignal()` methods |
| `src/engine/dozer-engine.ts` | `token?` param, sleep/signal error handlers |
| `src/client/dozer-client.ts` | `sendSignal()` method |
| `src/index.ts` | Export `DozerWorkflow`, `@NoStep`, new error types |

---

## Usage Example: VideosWorkflow refactored

```ts
@Workflow({ name: FlowType.IMG2VIDEO, resultQueue: { ... } })
export class VideosWorkflow extends DozerWorkflow<GeneratorContext> {

  constructor(
    private readonly s3: S3Storage,
    private readonly imageGeneratorService: ImageGeneratorService,
    private readonly videoGeneratorService: VideoGeneratorService,
  ) {
    super();
  }

  async run(input: GeneratorContext): Promise<VideosWorkflowResult> {
    const imageUrl = await this.generateImage(input);
    const imageKeys = await this.finalizeImage(input, imageUrl);
    const predictionId = await this.createVideoPrediction(input, imageKeys);

    // Option A: poll model — worker freed between checks
    let status: PredictionStatus | null = null;
    while (!status) {
      await this.sleep(10_000);
      status = await this.checkVideoStatus(predictionId);
    }

    // Option B: push model — wait for webhook
    // const status = await this.waitForSignal<PredictionStatus>('video-completed', {
    //   timeoutMs: 30 * 60 * 1000,
    // });
    // if (!status) throw new NonRetryableError('Video generation timed out');

    const videoKeys = await this.downloadProcessAndUpload(input, status.output);
    return { profile_id: input.profile_id, order_id: input.order_id, artifacts: { ... } };
  }

  @Step({ name: 'generate-image', retry: { attempts: 2 } })
  private async generateImage(input: GeneratorContext): Promise<string> { ... }

  @Step({ name: 'finalize-image', retry: { attempts: 5 } })
  private async finalizeImage(input: GeneratorContext, imageUrl: string): Promise<ResultKeys> { ... }

  @Step({ name: 'create-video-prediction', retry: { attempts: 2 } })
  private async createVideoPrediction(input: GeneratorContext, imageKeys: ResultKeys): Promise<string> { ... }

  @Step({ name: 'check-video-status' })
  private async checkVideoStatus(predictionId: string): Promise<PredictionStatus | null> {
    const status = await this.videoGeneratorService.getPredictionStatus(predictionId);
    if (status.status === 'failed' || status.status === 'canceled') {
      throw new NonRetryableError(`${status.error || 'Video generation failed'}`);
    }
    return status.status === 'succeeded' ? status : null;
  }

  @NoStep()
  private async downloadProcessAndUpload(
    input: GeneratorContext,
    output: string | string[] | undefined,
  ): Promise<VideoResultKeys> { ... }
}
```

---

## Out of Scope

- **Queries**: read-only workflow state inspection — separate future task
- **Updates**: sync signal with response — separate future task
- **Step retry non-blocking**: retry backoff currently blocks in-process — separate future task
- **Workflow cancellation of sleeping jobs**: cancel() currently doesn't promote delayed jobs — separate future task
