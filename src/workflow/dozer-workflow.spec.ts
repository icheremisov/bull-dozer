import { WorkflowExecutionContextStorage } from '../runtime/workflow-execution-context';
import { WorkflowSleepRequestedError } from '../errors/workflow-sleep-requested.error';
import { DozerWorkflow } from './dozer-workflow';

class ConcreteWorkflow extends DozerWorkflow<{ value: number }> {
  run(input: { value: number }): Promise<number> {
    return Promise.resolve(input.value);
  }

  exposeBreakUntil(ts: number): void {
    return this.breakUntil(ts);
  }

  exposeBreakFor(ms: number): void {
    return this.breakFor(ms);
  }

  async exposeWaitForSignal<T>(name: string): Promise<T | null> {
    return this.waitForSignal<T>(name);
  }
}

// ---------------------------------------------------------------------------
// breakUntil
// ---------------------------------------------------------------------------

describe('DozerWorkflow.breakUntil()', () => {
  it('throws WorkflowSleepRequestedError for a future timestamp', () => {
    const workflow = new ConcreteWorkflow();
    expect(() => workflow.exposeBreakUntil(Date.now() + 60_000)).toThrow(
      WorkflowSleepRequestedError,
    );
  });

  it('throws with the exact timestamp provided', () => {
    const workflow = new ConcreteWorkflow();
    const futureTs = Date.now() + 60_000;
    let error!: WorkflowSleepRequestedError;
    try {
      workflow.exposeBreakUntil(futureTs);
    } catch (e) {
      error = e as WorkflowSleepRequestedError;
    }
    expect(error.wakeUpAt).toBe(futureTs);
  });

  it('returns silently for a past timestamp (time has elapsed)', () => {
    const workflow = new ConcreteWorkflow();
    expect(() => workflow.exposeBreakUntil(Date.now() - 1)).not.toThrow();
  });

  it('does not require a workflow execution context', () => {
    // Deliberately called outside WorkflowExecutionContextStorage.run()
    const workflow = new ConcreteWorkflow();
    expect(() => workflow.exposeBreakUntil(Date.now() - 1)).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// breakFor
// ---------------------------------------------------------------------------

describe('DozerWorkflow.breakFor()', () => {
  it('throws WorkflowSleepRequestedError for a positive duration', () => {
    const workflow = new ConcreteWorkflow();
    expect(() => workflow.exposeBreakFor(60_000)).toThrow(
      WorkflowSleepRequestedError,
    );
  });

  it('throws with wakeUpAt ≈ Date.now() + durationMs', () => {
    const workflow = new ConcreteWorkflow();
    const before = Date.now();
    let error!: WorkflowSleepRequestedError;
    try {
      workflow.exposeBreakFor(5_000);
    } catch (e) {
      error = e as WorkflowSleepRequestedError;
    }
    expect(error.wakeUpAt).toBeGreaterThanOrEqual(before + 4_900);
    expect(error.wakeUpAt).toBeLessThanOrEqual(before + 6_000);
  });

  it('returns silently for zero duration', () => {
    const workflow = new ConcreteWorkflow();
    // Date.now() + 0 ≤ Date.now() → no throw
    expect(() => workflow.exposeBreakFor(0)).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// waitForSignal
// ---------------------------------------------------------------------------

describe('DozerWorkflow.waitForSignal()', () => {
  it('throws when outside workflow context', async () => {
    const workflow = new ConcreteWorkflow();
    await expect(workflow.exposeWaitForSignal('test')).rejects.toThrow(
      'waitForSignal() must be called within a workflow context',
    );
  });

  it('delegates to context when inside workflow context', async () => {
    const mockContext = {
      waitForSignal: jest.fn().mockResolvedValue({ amount: 42 }),
    };

    const workflow = new ConcreteWorkflow();
    const result = await WorkflowExecutionContextStorage.run(
      mockContext as never,
      () => workflow.exposeWaitForSignal<{ amount: number }>('payment'),
    );

    expect(mockContext.waitForSignal).toHaveBeenCalledWith(
      'payment',
      undefined,
    );
    expect(result).toEqual({ amount: 42 });
  });
});
