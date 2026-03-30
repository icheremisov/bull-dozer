import { WorkflowSleepRequestedError } from '../errors/workflow-sleep-requested.error';
import { WorkflowExecutionContextStorage } from '../runtime/workflow-execution-context';

interface JobRuntime {
  readonly priority: number;
  readonly progress: number | object;
  log(row: string): Promise<number>;
  clearLogs(keepLast?: number): Promise<void>;
  changePriority(opts: { priority?: number; lifo?: boolean }): Promise<void>;
  updateProgress(progress: number | object): Promise<void>;
}

export abstract class DozerWorkflow<TInput = unknown> {
  private _job?: JobRuntime;

  /**
   * @internal Called by the engine before run() to bind the job runtime
   * context. Not intended for use in workflow code.
   */
  _setJobContext(job: JobRuntime): void {
    this._job = job;
  }

  /** Current job priority (as set when the job was created or last changed). */
  protected get priority(): number {
    return this._job?.priority ?? 0;
  }

  /** Current job progress value. */
  protected get progress(): number | object {
    return this._job?.progress ?? 0;
  }

  /**
   * Append a log row to the BullMQ job log.
   * Returns the total number of log entries after the append.
   *
   * Log entries are stored in Redis and visible in Bull Board / BullMQ UI.
   * To cap the number of stored entries, set `keepLogs` in the job options
   * (via `@Workflow({ job: { keepLogs: N } })`).
   */
  protected log(row: string): Promise<number> {
    return this._job?.log(row) ?? Promise.resolve(0);
  }

  /**
   * Clear all log entries from the BullMQ job log.
   * Pass `keepLast` to retain the most recent N entries instead of clearing
   * all.
   */
  protected clearLogs(keepLast?: number): Promise<void> {
    return this._job?.clearLogs(keepLast) ?? Promise.resolve();
  }

  /**
   * Change the priority of the current job.
   * A lower `priority` value means higher precedence.
   * Set `lifo: true` to move the job to the tail of its priority bucket
   * (instead of the head) when using priority 0.
   */
  protected changePriority(opts: {
    priority?: number;
    lifo?: boolean;
  }): Promise<void> {
    return this._job?.changePriority(opts) ?? Promise.resolve();
  }

  /**
   * Update the progress of the current job.
   * Accepts a number (0–100) or an arbitrary object for structured progress.
   * Progress is stored in Redis and visible in Bull Board / BullMQ UI.
   */
  protected updateProgress(progress: number | object): Promise<void> {
    return this._job?.updateProgress(progress) ?? Promise.resolve();
  }

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
