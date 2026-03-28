# Workflow Failure Handling — Design Spec

**Date:** 2026-03-28
**Version target:** 0.6.0

## Overview

Add two coordinated failure-handling features to Dozer:

1. **Failure callbacks** — a per-workflow `onFailed()` method and a global `onWorkflowFailed` module option, both intended for alerting when a workflow reaches terminal failure.
2. **Failure result queue publishing** — an opt-in per-workflow option to publish a job to the result queue on failure, using the same queue and job structure as success, distinguished by a `status` field.

---

## Feature 1: Failure Callbacks

### Per-workflow `onFailed()` method

New file `src/workflow/workflow-with-failure-handler.ts`, exported from `src/index.ts`:

```ts
export interface WorkflowWithFailureHandler<TInput = unknown> {
  onFailed(error: Error, input: TInput, jobId: string): Promise<void>;
}
```

Users implement this interface on their workflow class:

```ts
@Workflow({ name: 'my-workflow' })
export class MyWorkflow implements WorkflowWithFailureHandler<MyInput> {
  async run(input: MyInput) { ... }

  async onFailed(error: Error, input: MyInput, jobId: string) {
    await this.alertService.send(`Workflow ${jobId} failed: ${error.message}`);
  }
}
```

The engine detects the method via duck-typing (`typeof workflow.onFailed === 'function'`). The `implements` declaration is optional but recommended for IDE support and type safety.

### Global `onWorkflowFailed` callback

Added to `DozerModuleOptions`:

```ts
export interface DozerModuleOptions {
  // ...existing fields...
  onWorkflowFailed?: (
    jobId: string,
    workflowName: string,
    error: Error,
  ) => Promise<void> | void;
}
```

Usage:

```ts
DozerModule.forRoot({
  queue,
  onWorkflowFailed: async (jobId, workflowName, error) => {
    await sentry.captureException(error, { jobId, workflowName });
  },
})
```

### Trigger conditions

Both `onFailed()` and `onWorkflowFailed` are called when a workflow reaches terminal failure — i.e., when `stateStore.markFailed()` is called. This includes:

- All step retries exhausted
- `NonRetryableError` thrown (no retries attempted)
- All workflow-level retries exhausted

They are **not** called on intermediate retry attempts.

---

## Feature 2: Failure Result Queue Publishing

### New option on `WorkflowResultQueueOptions`

```ts
export interface WorkflowResultQueueOptions {
  jobName?: string;
  job?: WorkflowJobOptions;
  publishOnFailure?: boolean; // default: false
}
```

When `publishOnFailure: true`, the engine publishes a job to the result queue on terminal failure, in addition to (or instead of) the existing success path.

### Updated `WorkflowResultQueueJobData`

```ts
export interface WorkflowResultQueueJobData<TResult = unknown> {
  jobId: string;
  workflowName: string;
  status: 'completed' | 'failed'; // new
  result: TResult | null;         // was TResult, now nullable
  error?: string;                 // new, present when status === 'failed'
}
```

### Updated `WorkflowResultMessage`

```ts
export interface WorkflowResultMessage<TResult = unknown> {
  resultJobId: string;
  resultJobName: string;
  workflowJobId: string;
  workflowName: string;
  status: 'completed' | 'failed'; // new
  result: TResult | null;         // was TResult, now nullable
  error?: string;                 // new
}
```

### Failure payload

On terminal failure with `publishOnFailure: true`:

```ts
{
  jobId: job.id,
  workflowName: job.name,
  status: 'failed',
  result: null,
  error: error.message,
}
```

The result queue job ID uses the same `toWorkflowResultQueueJobId(job.id)` mapping as success — one result queue entry per workflow job.

---

## Engine Changes

### `handleWorkflowFailure` private method

New private method on `DozerEngine`:

```ts
private async handleWorkflowFailure(
  job: WorkflowJob<unknown>,
  definition: RegisteredWorkflow,
  workflow: unknown,
  input: unknown,
  error: Error,
): Promise<void>
```

Executes three independent actions sequentially. Errors in any action are suppressed silently (`try/catch` with no rethrow) and do not prevent the original error from being thrown.

1. **Call `workflow.onFailed()`** — if the workflow instance has an `onFailed` method.
2. **Call global `onWorkflowFailed`** — if configured in module options.
3. **Publish to result queue** — if `definition.options.resultQueue?.publishOnFailure === true` and `moduleOptions.resultQueue` is set.

### Tracking workflow instance in `engine.run()`

`lastWorkflow` and `lastInput` variables declared outside the `while` loop, assigned inside `try` after `instantiate()`. Used by `handleWorkflowFailure` in the catch block.

### Updated catch block flow

```
catch(error):
  if WorkflowResultPublishStageError → rethrow cause
  if WorkflowResultFinalizeStageError → rethrow cause
  if WorkflowDeterminismProbeStageError → rethrow cause
  if WorkflowRetryRequestedError → sleep + continue
  if not NonRetryableError AND attempts remain → sleep + continue

  // terminal failure:
  stateStore.markFailed(error)
  if (definition) {
    await handleWorkflowFailure(job, definition, lastWorkflow, lastInput, error)
  }
  throw error
```

---

## Tests

Added to `src/dozer-engine.spec.ts`:

| Scenario | Expected |
|---|---|
| `onFailed` called with correct `(error, input, jobId)` after all step retries exhausted | ✓ |
| `onFailed` called when `NonRetryableError` thrown | ✓ |
| `onFailed` NOT called on intermediate step retry attempts | ✓ |
| Error thrown inside `onFailed` does not suppress original error | ✓ |
| Global `onWorkflowFailed` called on terminal failure | ✓ |
| Global `onWorkflowFailed` called when `NonRetryableError` thrown | ✓ |
| Error thrown inside global callback does not suppress original error | ✓ |
| Workflow without `onFailed` method — no crash | ✓ |
| `publishOnFailure: true` → result queue receives `status: 'failed'` job | ✓ |
| `publishOnFailure: false` (default) → nothing published on failure | ✓ |
| Success path → result queue receives `status: 'completed'` with correct `result` | ✓ |
| `WorkflowResultMessage` correctly decoded for both `completed` and `failed` statuses | ✓ |

---

## Breaking Changes (v0.6.0)

- `WorkflowResultQueueJobData.result` type changes from `TResult` to `TResult | null`
- `WorkflowResultQueueJobData` gains required `status: 'completed' | 'failed'` field
- `WorkflowResultMessage` gains `status` and optional `error` fields; `result` becomes nullable

Existing `createResultWorker` handlers will receive `status: 'completed'` and `result: TResult` for successful workflows — behavior unchanged, but types must be updated.

---

## Files Changed

| File | Change |
|---|---|
| `src/workflow/workflow-with-failure-handler.ts` | New — `WorkflowWithFailureHandler` interface |
| `src/queue/workflow-queue.ts` | Extend `WorkflowResultQueueJobData` with `status`, `error` |
| `src/decorators/workflow.decorator.ts` | Add `publishOnFailure` to `WorkflowResultQueueOptions` |
| `src/client/workflow-result-worker.ts` | Extend `WorkflowResultMessage` with `status`, `error` |
| `src/engine/dozer-engine.ts` | Add `handleWorkflowFailure`, update catch block, track `lastWorkflow`/`lastInput` |
| `src/dozer.module.ts` | Add `onWorkflowFailed` to `DozerModuleOptions` |
| `src/index.ts` | Export `WorkflowWithFailureHandler` |
| `src/dozer-engine.spec.ts` | New test cases |
| `package.json` | Version bump to `0.6.0` |
