# dozer Test Matrix

This document describes expected behavior in test terms:
- `Allowed`: must work
- `Rejected`: must fail or be detected
- `Accepted Tradeoff`: behavior intentionally tolerated

## Allowed (must work)

| Behavior | Test expectation | Covered by |
|---|---|---|
| Start workflow through HTTP/API | `POST /workflows/:name/start` creates a job and reaches terminal state | `example/test/workflows.integration.spec.ts`, `example/test/workflows.e2e.spec.ts` |
| Deterministic happy path | Steps execute in order and persist results into compact state (`d.c`) | `runs simple workflow and stores compact state` |
| Replay after failure | Completed steps are reused from cache and not executed again | `replays workflow and restores state without re-running completed step`, `restores workflow state and replays completed steps only once` |
| Nested steps indexing | Step keys are built as `0:*`, `0.0:*`, `0.0.0:*`, ... | `stores nested step indexes and repeated step calls distinctly`, `supports deep nested recursion` |
| Repeated calls to same step method | Each invocation gets a unique step index and cache slot | `handles repeated calls of the same step method as separate step keys` |
| Step retry/backoff | Unstable step can still succeed under retry policy | `retries unstable steps by retry policy`, `handles deterministic and random failures in flaky workflow` |
| Backoff strategy math | `constant`, `linear`, `exponential` delays are computed correctly | `runtime/retry-policy.spec.ts` |
| Execution defaults and overrides | Module defaults apply and workflow-level options can override | `applies module-level default workflowRetry options`, `lets workflow-level workflowRetry options override module defaults` |
| Long-running replay | Long workflow can fail and later complete on replay | `supports long-running workflow with fail then replay` |
| Heterogeneous workflow input | Different input shapes/types are accepted | `supports various workflow input types` |
| Heterogeneous step output | `void`, `undefined`, primitives and objects are persisted correctly | `persists steps that return void/undefined and typed values` |
| Mixed sync/async steps | Sync and async step methods can coexist in one workflow | `supports mix of sync and async steps` |
| Sequential batch with shared await | Sequential processing with aggregate result stays stable | `supports sequential batch execution with shared await` |
| Nondeterminism inside a step | Step randomness is stable on replay because step result is cached | `keeps action-level nondeterministic result stable on replay` |
| Workflow with no `@Step` methods | `run`-only workflow is valid | `supports workflow without @Step methods` |
| Step call outside workflow runtime | Decorated method works as a plain method without context | `supports step calls outside workflow run` |
| Plain method without `@Step` | Method is not cached and is re-executed on replay | `re-executes plain methods that are not decorated with @Step` |
| Inheritance and polymorphism | Overridden step methods are resolved and cached by actual dispatch | `supports workflow inheritance and polymorphic step override`, `supports inherited dispatch when override has @Step with different name` |
| Nested workflows | Parent can start child workflow and await completion | `supports nested workflow invocation and waits for child completion` |
| Nested workflow failure propagation | Child failure propagates, parent can recover on replay | `propagates child workflow failures and recovers parent workflow on replay` |
| Deep nesting (`parent -> child -> grandchild`) | Deep workflow hierarchy works with random timings and replay | `supports deep workflow nesting with random step timing and replay` |
| `this` state restoration | Workflow-local state can be reconstructed from cached step outputs | `restores workflow-local this-state via cached step results` |
| Binary payload serialization | `Uint8Array`/`ArrayBuffer`/`Buffer`/`Blob` survive start/run/replay | `supports binary arguments and fails on non-serializable payloads` |
| `Date` serialization | `Date` values survive serialization and replay | `serializes Date values and restores cached step result on replay` |
| Step timeout with compensation | `@Step({ timeout })` raises `TimeoutError` and compensation runs in `run` | `supports timeout handling with compensating actions in workflow run` |
| Workflow auto-resume | Workflow resumes automatically based on `execution.workflowRetry` | `automatically resumes workflow by workflowRetry settings` |
| Client-only starter module | Workflow can be enqueued from service that does not host workers | `starts workflows from client-only module without worker providers` |
| Observability endpoint | Bull Board endpoint is reachable | `exposes BullMQ dashboard endpoint`, `serves Bull Board endpoint` |
| Status API | Status endpoint returns `found` and compact state payload | `restores workflow state from Redis through status endpoint` |

## Rejected (must fail or be detected)

| Behavior | Expected result | Covered by |
|---|---|---|
| Invalid workflow input | Workflow ends in `failed`, error stored in `d.e` | `fails workflow run with invalid input parameters` |
| `NonRetryableError` from step | Step must not be retried even when retry policy exists | `does not retry step when NonRetryableError is thrown` |
| Unknown workflow id/name | Job fails with "not registered" error | `fails job execution for invalid workflow identifier`, `marks state as failed when workflow is not registered` |
| Nondeterministic run logic | Replay divergence must fail with replay conflict / nondeterminism error | `detects non-deterministic replay divergence`, `detects run-level nondeterminism...` |
| Logic version skew during unfinished jobs | Replay must detect step trace conflict | `detects version skew when workflow logic changes before replay` |
| Non-serializable workflow input | `start()` throws `SerializationError` | `rejects non-serializable workflow input values` |
| Non-serializable step result | Workflow fails and stores error state | `fails workflow when step result is non-serializable` |
| Duplicate workflow names | Workflow registration fails | `fails when multiple workflows share the same name` |

## Accepted Tradeoff (expected degradation)

| Behavior | Expected result | Covered by |
|---|---|---|
| Random flaky workflow | Terminal state may be either `completed` or `failed` | `handles deterministic and random failures in flaky workflow` |
| Unknown status job id | API returns `{ found: false }` instead of HTTP 404 | `returns not found status for unknown workflow job id` |

## Explicitly Out of Scope (not guaranteed by current tests)

| Area | Note |
|---|---|
| True parallel execution of multiple `@Step` calls inside one `run` | Current tests focus on deterministic sequential orchestration |
| Multi-queue cross-orchestration | Test suite primarily validates one workflow queue path |
| State schema migration between library versions | Tests validate replay conflict detection, not automatic schema migrations |
