# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

---

## [0.8.1] — 2026-03-30

### Added

- **`DozerEngine.start(name, input, jobOptions?)`** — third optional parameter accepts `WorkflowJobOptions` (BullMQ `JobsOptions`) per-call, including `jobId` for deterministic IDs. Options are merged with lowest priority: global defaults → workflow-level → per-call overrides. `DozerClient.start()` already accepted this parameter.

---

## [0.8.0] — 2026-03-30

### Added

- **`DozerWorkflow` job runtime methods** — workflows can now interact with the underlying BullMQ job directly via protected methods on the base class:
  - `log(row)` — appends a row to the BullMQ job log (visible in Bull Board / BullMQ UI). Returns the total log count after append.
  - `clearLogs(keepLast?)` — clears all job log entries, or retains the most recent `keepLast` entries.
  - `updateProgress(progress)` — sets job progress (number 0–100 or an arbitrary object for structured progress).
  - `changePriority({ priority?, lifo? })` — changes the job's queue priority at runtime.
  - `priority` getter — current job priority (as set when created or last changed).
  - `progress` getter — current job progress value.
- **`_setJobContext(job)`** — called by the engine before every `run()` invocation (including replays) to bind the live BullMQ job to the workflow instance. Not intended for use in workflow code.
- **`WorkflowQueueDriver.getJobLogs(jobId)`** — queue-level log retrieval added to the driver interface, returning `{ logs: string[]; count: number }`. Implemented in both `BullMQWorkflowQueue` (delegates to `queue.getJobLogs`) and `InMemoryWorkflowQueue` (in-memory accumulation).
- **`keepLogs` enforcement in `InMemoryWorkflowJob`** — `InMemoryWorkflowJob.log()` now trims the in-memory log array to the last `keepLogs` entries (when `keepLogs > 0`), matching BullMQ's Redis-side Lua trim semantics. `keepLogs: 0` or `undefined` means unlimited.

### Tests

- `DozerEngine` job-methods unit tests (`dozer-engine-job-methods.spec.ts`) — 16 tests covering `log`, `clearLogs`, `updateProgress`, `changePriority`, and `keepLogs` option enforcement across five workflow fixtures.
- `DozerEngine` concurrency unit tests (`dozer-engine-concurrency.spec.ts`) — 2 tests: 10 parallel jobs with no `_job` cross-contamination, and `breakUntil` re-instantiation preserving `_job` context on resume.
- Integration tests (real BullMQ + Redis, `example/test/job-methods.integration.spec.ts`) — 5 tests against a live Redis queue: `log()` entries visible via `queue.getJobLogs()`, `updateProgress()` final value is 100, `clearLogs()` wipes earlier entries mid-run, `changePriority()` reflected on the completed job, `getJobLogs()` returns empty for unknown job ID.
- Parallel isolation integration tests (`example/test/parallel-isolation.integration.spec.ts`) — 3 tests verifying that 10–20 concurrently running workflows with different coefficients, deliberate failures (retry), and sleep/resume produce mathematically correct independent results with no cross-job context leakage: isolated step cache, isolated logs, no ID mixing.
- Example workflows `job-runtime` and `parallel-stress` added to the example project as test fixtures.

---

## [0.7.3] — 2026-03-30

### Added

- **`breakUntil(timestamp)`** — pure stateless interrupt: throws `WorkflowSleepRequestedError` if `Date.now() < timestamp`, returns silently otherwise. No state is written; determinism relies on the timestamp coming from workflow input or a cached `@Step` result.
- **`breakFor(durationMs)`** — convenience wrapper around `breakUntil(Date.now() + durationMs)`. Safe to call inside an uncached `@Step` body (polling pattern) where the step re-executes from scratch on each resume.
- **Polling pattern** — a `@Step` with `while(true)` + `breakFor()` inside re-executes its body on every resume. The trace records exactly **one** entry for the step regardless of how many polling iterations are needed, preventing unbounded trace growth.
- **`traceEnabled` config** — set `execution: { traceEnabled: false }` per `@Workflow` or via `defaults.execution` in `DozerModule.forRoot` to disable trace recording and `StepReplayConflictError` detection. Useful for workflows with legitimate dynamic step ordering.
- **`maxStateSizeBytes` config** — module-level byte limit on serialized job state. Throws `StateSizeLimitError` (with `actualBytes` / `limitBytes` fields) and marks the job as failed when the limit is exceeded.
- **`StateSizeLimitError`** — exported error class, thrown when `maxStateSizeBytes` is exceeded.
- **`StepReplayConflictError`** — exported error class, thrown when a replayed workflow calls steps in a different order than recorded in the trace.

### Changed

- `sleep()` / `sleepUntil()` / `pause()` / `pauseUntil()` removed from `DozerWorkflow`. Replace with `breakUntil(deterministicTimestamp)` or `breakFor(durationMs)`.
- `saveSleepIntent` / `getSleepIntent` / `clearSleepIntent` removed from the state store. The `sl` field in `CompactWorkflowState` is retained for backward compatibility with jobs already in Redis but is no longer written.

### Tests

- `DozerWorkflow` unit tests — `breakUntil` and `breakFor` behaviour (future/past timestamps, wakeUpAt precision, zero-duration no-op, context-free usage).
- `DozerEngine` sleep/break integration tests — `DelayedError` on park, `state.sl` never written, real 100 ms wait + promote cycle.
- `DozerEngine` polling integration tests — `state.sl` never written, trace length stays at 1 across iterations, step body re-executes on each resume, failure propagation.
- `DozerEngine` config tests — `traceEnabled: true/false` (per-workflow and module-level), `StepReplayConflictError` triggered / suppressed, `maxStateSizeBytes` within/over limit, boundary (limit = actualSize − 1).
- Integration tests (real BullMQ + Redis) — polling workflow completes through pending statuses and completes immediately when status is already terminal.

---

## [0.7.0] — 2026-03-29

### Fixed

- Engine `run()` now wraps non-`Error` throws (strings, numbers, etc.) via `asThrownError()` before re-throwing. Previously a step throwing `'plain string'` or `404` propagated the raw primitive, making it impossible to catch as an `Error` instance.

### Tests

- `value-serializer` — 26 unit tests covering all supported types (null, string, number, boolean, undefined, Date, Buffer, ArrayBuffer, Uint8Array, DataView, all 8 typed arrays), nested objects/arrays, round-trip fidelity, and all `SerializationError` cases (bigint, function, symbol, class instance, circular reference, invalid Date).
- `WorkflowRegistry` — 12 unit tests for `register`, `resolveDefinition`, `resolve`, and `resolveOptionalDefinition`, including duplicate-name guard and missing `run()` method error.
- `getWorkflowJobInfo` — 18 unit tests covering all 6 status codes → `statusName` mapping, `result`/`error` field presence rules, and `isCompactWorkflowState` fallback for corrupted/missing state.
- `DozerModule.forRootAsync` — 2 integration tests: async factory initialization and dependency injection via `imports`.

---

## [0.6.0] — 2026-03-28

### Added

- **`DozerWorkflow` abstract base class** — workflow classes must now extend `DozerWorkflow<TInput>`. Provides `sleep()`, `sleepUntil()`, and `waitForSignal()` as protected methods.
- **`@NoStep()` decorator** — marks a method on a `@Workflow` class as a plain helper (not a durable step), suppressing the validator warning.
- **Durable sleep** — `this.sleep(ms)` and `this.sleepUntil(timestamp)` pause execution and re-queue the job as a BullMQ delayed job. The wakeup time is persisted in `CompactWorkflowState.sl` so replays are deterministic.
- **Signal waiting** — `this.waitForSignal<T>(name, { timeoutMs? })` parks the workflow until `DozerClient.sendSignal(jobId, name, payload)` is called. Pending signal state is stored in `CompactWorkflowState.ps`. Returns `null` on timeout.
- **`DozerClient.sendSignal(jobId, signalName, payload?)`** — delivers a signal to a parked workflow, stores the payload in the step cache, and promotes the delayed job back to the active queue. Returns `false` if no matching pending signal is registered.
- **`defaults.signalTimeoutMs`** — new module option that sets a global deadline for `waitForSignal()` calls that have no per-call `timeoutMs`.
- **`WorkflowQueueDriver.moveToDelayed` / `promoteDelayed`** — new required methods on the queue driver interface, implemented in both `InMemoryWorkflowQueue` and `BullMQWorkflowQueue`.
- **`WorkflowWithFailureHandler` interface** — type-safe contract for workflows that implement `onFailed(error, input, jobId)`.
- **`onWorkflowFailed` module option** — global callback invoked on every terminal workflow failure (errors inside the callback are suppressed).
- **`publishOnFailure` result queue option** — when `true`, a failure payload `{ status: 'failed', error: string }` is published to the result queue on terminal failure.
- **`status` and `error` fields in result queue payload** (breaking) — result queue jobs now always include `status: 'completed' | 'failed'` and `error?: string`.

### Changed

- `@Workflow` decorator now validates that all methods on the class are either annotated with `@Step`, `@NoStep`, or inherited from `DozerWorkflow`. Unknown plain methods cause a startup error.
- `CompactWorkflowState` extended with `sl` (sleep intents) and `ps` (pending signals) fields.

### Fixed

- Removed unsafe `as any` cast in `DozerWorkflow`.
- `deliverSignal` now flushes state atomically before promoting the delayed job.

---

## [0.5.0] — 2026-02-26

### Added

- Redis cluster prefix support — `BullMQWorkflowQueue` now accepts a `prefix` option that is forwarded to the underlying BullMQ `Queue`.

---

## [0.4.0] — 2026-02-25

### Added

- **Workflow result queue** — workflows can publish their return value to a separate BullMQ queue via `resultQueue: { jobName }` in `@Workflow` options. Consumers use `DozerClient.getResult()`, `hasResult()`, and `waitForResult()`.
- `DozerClient.getJobInfo(jobId)` — returns workflow status, result, and error from job state.
- `DozerClient.cancel(jobId)` — cancels a pending or running workflow.
- `decodeWorkflowResultJob` / `createWorkflowResultProcessor` — helpers for processing result queue jobs.
- `toWorkflowResultQueueJobId` — utility to derive the deterministic result queue job ID from a workflow job ID.

---

## [0.3.0] — 2026-02-24

### Added

- Initial public release of the Dozer workflow engine for NestJS / BullMQ.
- `DozerModule.forRoot` / `forRootAsync` / `forFeature` / `forClient` / `forClientAsync`.
- `DozerEngine` — executes durable workflows with step caching and retry support.
- `DozerClient` — starts workflows and queries job state.
- `@Workflow` / `@Step` decorators.
- `InMemoryWorkflowQueue` — in-process queue driver for unit tests.
- `NonRetryableError` — marks an error as non-retryable so the engine skips remaining attempts.
- Retry policy with `fixed`, `exponential`, and `linear` strategies.
