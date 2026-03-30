import { WorkflowSleepRequestedError } from '../errors/workflow-sleep-requested.error';
import { WorkflowExecutionContextStorage } from '../runtime/workflow-execution-context';

export abstract class DozerWorkflow<TInput = unknown> {
  abstract run(input: TInput): Promise<unknown>;

  /**
   * Synchronously interrupts workflow execution until the given absolute
   * timestamp. If the timestamp is still in the future the call throws
   * `WorkflowSleepRequestedError`, causing the engine to park the job and
   * re-queue it at that time. If the timestamp has already passed the call
   * returns immediately (no-op).
   *
   * **The timestamp MUST be deterministic** — derived from workflow input or
   * a cached `@Step` result — so that every replay produces the same
   * decision. Using `Date.now()` directly here will cause the workflow to
   * park on every replay run.
   *
   * No `await` needed. The call either throws (interrupting execution) or
   * returns silently (time has passed, continuing).
   *
   * @example
   * // Correct: timestamp comes from a cached step
   * const wakeUpAt = await this.scheduleResume(10_000); // @Step
   * this.breakUntil(wakeUpAt);
   *
   * @example
   * // Correct: timestamp comes from workflow input
   * this.breakUntil(input.resumeAt);
   */
  protected breakUntil(timestamp: number): void {
    if (Date.now() < timestamp) {
      throw new WorkflowSleepRequestedError(timestamp);
    }
  }

  /**
   * Convenience wrapper: interrupts execution for `durationMs` milliseconds.
   * Equivalent to `this.breakUntil(Date.now() + durationMs)`.
   *
   * **Requires a deterministic call site.** The computed timestamp is fresh on
   * every replay run, so this method should only be used where the caller's
   * position in the workflow is reached for the first time (i.e. the
   * surrounding `@Step` has not yet been cached). For a guaranteed-deterministic
   * delay, capture the timestamp in a `@Step` first and use `breakUntil`.
   */
  protected breakFor(durationMs: number): void {
    this.breakUntil(Date.now() + durationMs);
  }

  protected async waitForSignal<T>(
    signalName: string,
    opts?: { timeoutMs?: number },
  ): Promise<T | null> {
    const context = WorkflowExecutionContextStorage.get();
    if (!context) {
      throw new Error(
        'waitForSignal() must be called within a workflow context',
      );
    }
    return context.waitForSignal(signalName, opts);
  }
}
