# dozer Implementation Plan

## Status

- [x] Plan created and saved to file.
- [x] Implementation started.

## 1. Target Architecture and Boundaries

- [x] Define that the root project is the `dozer` library, not a demo app.
- [x] Define that `example` is a separate NestJS project inside the repository.
- [x] Define the execution contract: `start()` enqueues a job, execution is handled by a worker.
- [x] Define the compact-state contract (`i`, `d.s`, `d.c`, `d.r`, `d.e`).

## 2. Project Cleanup

- [x] Remove unused Nest starter files from root (if not needed by the library).
- [x] Restructure `src` for a library-first layout.
- [x] Add/verify `src/index.ts` with public exports.
- [x] Make library `package.json` publish-ready.
- [x] Ensure only truly unused files are removed.

## 3. Library Core as a NestJS Module

- [x] Finalize `DozerModule.forRoot/forRootAsync/forFeature` API.
- [x] Add client-only API (`DozerModule.forClient/forClientAsync` + `DozerClient`) for starting workflows from a separate project.
- [x] Keep workflow registration DI-based via Nest providers only.
- [x] Harden BullMQ wrapper (queue/worker adapter) for production usage.
- [x] Add domain errors (`WorkflowNotRegistered`, `StepReplayConflict`, `NonDeterminismError`, `SerializationError`).
- [x] Add `NonRetryableError` and `TimeoutError`.
- [x] Improve compact-state storage to avoid duplication.
- [x] Implement descendant cache cleanup when parent step result is saved (`c/u/a`).
- [x] Implement and verify replay nondeterminism detection.
- [x] Implement retry backoff strategies (`constant`, `linear`, `exponential`).
- [x] Add workflow auto-resume via execution settings (`execution.workflowRetry`).

## 4. `example` Project Setup

- [x] Initialize a standalone NestJS project in `/example`.
- [x] Connect root library as module dependency (`file:..`).
- [x] Add endpoints for workflow start/status/replay.
- [x] Add multiple workflows for demonstration.

## 5. BullMQ + Redis + Dashboard in `example`

- [x] Add `example/docker-compose.yml` with Redis.
- [x] Configure BullMQ queue + worker against real Redis.
- [x] Integrate Bull Board (`/admin/queues`).
- [x] Add scripts for Redis lifecycle and integration tests.
- [x] Add dedicated Redis flush command and run it once before tests.
- [x] Ensure integration/e2e tests run on real Redis.

## 6. Workflow Scenarios for Examples and Tests

- [x] `SimpleWorkflow` (happy path).
- [x] `TypedInputWorkflow` (different input types).
- [x] `TypedStepWorkflow` (different step return types including `void/undefined`).
- [x] `NestedStepsWorkflow` (nested steps and indexing).
- [x] `FlakyWorkflow` (random errors + retry/backoff).
- [x] `LongRunningWorkflow` (long steps, failure, recovery).
- [x] `ReplayWorkflow` (re-run after fail).
- [x] `NonDeterministicWorkflow` (nondeterminism detection).

## 7. Full Test Coverage by System Aspect

- [x] DI workflow registration.
- [x] Job enqueue flow and `job.data` structure.
- [x] Worker execution on real Redis.
- [x] Step wrapping and prevention of re-executing completed steps.
- [x] Nested step indexing.
- [x] Different workflow input types.
- [x] Different step argument/result types.
- [x] Repeated step method call in one run.
- [x] Retry/backoff.
- [x] Random failures and state consistency.
- [x] Long-running workflows + process restart.
- [x] Workflow replay.
- [x] Workflow state restoration from Redis.
- [x] Nondeterministic behavior detection.
- [x] Compact-state format (minimum duplication).
- [x] BullMQ dashboard availability.
- [x] Workflow without steps (`run` only).
- [x] Run-level nondeterminism (timers/random/external source) and detection.
- [x] Preserve/restore `this` state on replay.
- [x] Guarantee completed step is not re-called on resume.
- [x] Deep nesting + recursion.
- [x] Nondeterminism inside action/step (must be safe via step cache).
- [x] Start and await nested workflows.
- [x] Mixed sync/async steps.
- [x] Behavior when method is not marked with `@Step`.
- [x] Behavior when workflow logic changes before old run completes.
- [x] Start workflow with invalid input.
- [x] Start workflow with invalid workflow identifier (job left after workflow removal).
- [x] Multiple methods with identical step names.
- [x] Multiple workflows with identical names (registration error).
- [x] Workflow class inheritance and step polymorphism.
- [x] Calling step methods outside workflow context (constructor/direct call).
- [x] Binary argument/parameter handling (`ByteArray`/`UintArray`/`ArrayBuffer`/`Blob`/`Buffer`) with correct replay.
- [x] Explicit serialization errors for unsupported values (functions/symbols/bigint/circular/unsupported object types).
- [x] Inheritance: overridden method called from base method with `@Step` using a different name.
- [x] Inheritance: overridden method without `@Step` is not cached and re-executes on replay.
- [x] Nested workflow: child failure is correctly propagated to parent and recovered on replay.
- [x] Deeper workflow nesting (`parent -> child -> grandchild`) with random step delays.
- [x] Batch mode: sequential step execution with shared await of final result.
- [x] `Date` serialization in inputs/results and `Date` restoration on replay.
- [x] `NonRetryableError` handling: step is not retried even with retry policy.
- [x] Step timeout (`@Step({ timeout })`) and compensation via `try/catch` in `run`.
- [x] Workflow auto-resume via `execution.workflowRetry` without manual replay.
- [x] Client-only workflow start (`DozerClient`) without worker providers.

## 8. Test Infrastructure Organization

- [x] Split tests into `unit`, `integration`, `e2e`.
- [x] Add fixtures/utilities for fault injection and replay.
- [x] Configure automatic Redis usage for integration/e2e.
- [x] Build unified pipeline: lint -> unit -> integration -> e2e.
- [x] Add benchmark scenarios (`single-step`, `multi-step`, `resume`).
- [x] Add real-Redis perf runner with parameters: steps, payload, job count, failure rate, nested workflows, step timers.

## 9. Implementation Order

- [x] Step 1: cleanup and library structure.
- [x] Step 2: core stabilization + nondeterminism.
- [x] Step 3: `example` app.
- [x] Step 4: Redis/BullMQ/Bull Board.
- [x] Step 5: full test suite.
- [x] Step 6: final hardening and documentation.

---

This file is updated during implementation: completed items are marked as `[x]`.
Local confirmation for real Redis runs was obtained: `example` tests (`integration` and `e2e`) pass on a live Redis instance.
