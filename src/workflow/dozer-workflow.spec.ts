import { WorkflowExecutionContextStorage } from '../runtime/workflow-execution-context';
import { DozerWorkflow } from './dozer-workflow';

class ConcreteWorkflow extends DozerWorkflow<{ value: number }> {
  run(input: { value: number }): Promise<number> {
    return Promise.resolve(input.value);
  }

  async exposeSleep(ms: number): Promise<void> {
    return this.sleep(ms);
  }

  async exposeSleepUntil(ts: number): Promise<void> {
    return this.sleepUntil(ts);
  }

  async exposeWaitForSignal<T>(name: string): Promise<T | null> {
    return this.waitForSignal<T>(name);
  }
}

describe('DozerWorkflow', () => {
  it('sleep() calls setTimeout when outside workflow context', async () => {
    const workflow = new ConcreteWorkflow();
    const start = Date.now();
    await workflow.exposeSleep(50);
    expect(Date.now() - start).toBeGreaterThanOrEqual(40);
  });

  it('waitForSignal() throws when outside workflow context', async () => {
    const workflow = new ConcreteWorkflow();
    await expect(workflow.exposeWaitForSignal('test')).rejects.toThrow(
      'waitForSignal() must be called within a workflow context',
    );
  });

  it('sleep() delegates to context.sleep() when inside workflow context', async () => {
    const mockContext = {
      sleep: jest.fn().mockResolvedValue(undefined),
    };

    const workflow = new ConcreteWorkflow();
    await WorkflowExecutionContextStorage.run(mockContext as never, () =>
      workflow.exposeSleep(100),
    );

    expect(mockContext.sleep).toHaveBeenCalledWith(100);
  });

  it('sleepUntil() delegates to sleep() with remaining ms for future timestamp', async () => {
    const mockContext = {
      sleep: jest.fn().mockResolvedValue(undefined),
    };

    const workflow = new ConcreteWorkflow();
    const futureTs = Date.now() + 5000;

    await WorkflowExecutionContextStorage.run(mockContext as never, () =>
      workflow.exposeSleepUntil(futureTs),
    );

    const calledMs = (mockContext.sleep.mock.calls[0] as [number])[0];
    expect(calledMs).toBeGreaterThan(4000);
    expect(calledMs).toBeLessThanOrEqual(5000);
  });

  it('sleepUntil() calls sleep(0) when timestamp is in the past', async () => {
    const mockContext = {
      sleep: jest.fn().mockResolvedValue(undefined),
    };

    const workflow = new ConcreteWorkflow();
    const pastTs = Date.now() - 1000;

    await WorkflowExecutionContextStorage.run(mockContext as never, () =>
      workflow.exposeSleepUntil(pastTs),
    );

    expect(mockContext.sleep).toHaveBeenCalledWith(0);
  });
});
