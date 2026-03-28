import { DOZER_JOB_INPUT_KEY, DOZER_JOB_STATE_KEY } from '../constants';
import { WORKFLOW_STATUS } from '../queue/workflow-queue';
import type { WorkflowJob, WorkflowJobData } from '../queue/workflow-queue';
import { WorkflowExecutionContext } from './workflow-execution-context';
import { WorkflowStateStore } from './workflow-state.store';
import { WorkflowSleepRequestedError } from '../errors/workflow-sleep-requested.error';
import { WorkflowSignalWaitRequestedError } from '../errors/workflow-signal-wait-requested.error';

const makeJob = (overrides?: Partial<{
  s: number; c: Record<string, unknown>; a?: Record<string, number>;
  u?: Record<string, 1>; t: string[]; sl?: Record<string, number>;
  ps?: Record<string, { k: string; e?: number }>;
}>): WorkflowJob<unknown> => {
  let data: WorkflowJobData<unknown> = {
    [DOZER_JOB_INPUT_KEY]: {},
    [DOZER_JOB_STATE_KEY]: {
      s: WORKFLOW_STATUS.running,
      c: {},
      t: [],
      ...overrides,
    } as any,
  };
  return {
    id: 'test-job',
    name: 'test',
    get data() { return data; },
    updateData: async (next: WorkflowJobData<unknown>) => { data = next; },
  };
};

describe('WorkflowExecutionContext.sleep()', () => {
  it('throws WorkflowSleepRequestedError on first call', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);
    await expect(ctx.sleep(5000)).rejects.toBeInstanceOf(WorkflowSleepRequestedError);
  });

  it('sets wakeUpAt approximately to now + durationMs', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);
    const before = Date.now();
    let error!: WorkflowSleepRequestedError;
    try {
      await ctx.sleep(5000);
    } catch (e) {
      error = e as WorkflowSleepRequestedError;
    }
    expect(error.wakeUpAt).toBeGreaterThanOrEqual(before + 4900);
    expect(error.wakeUpAt).toBeLessThanOrEqual(before + 6000);
  });

  it('saves wakeUpAt in sl state', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);
    try { await ctx.sleep(1000); } catch {}
    expect(job.data[DOZER_JOB_STATE_KEY]?.sl).toBeDefined();
  });

  it('completes sleep and returns when wakeUpAt has passed (simulated resume)', async () => {
    const wakeUpAt = Date.now() - 1000;
    const stepKey = '0:__sleep__';
    const job = makeJob({ sl: { [stepKey]: wakeUpAt }, t: [stepKey] });
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);
    await expect(ctx.sleep(1000)).resolves.toBeUndefined();
    expect(job.data[DOZER_JOB_STATE_KEY]?.sl?.[stepKey]).toBeUndefined();
    expect(job.data[DOZER_JOB_STATE_KEY]?.u?.[stepKey]).toBe(1);
  });

  it('returns immediately on replay (step already in u)', async () => {
    const stepKey = '0:__sleep__';
    const job = makeJob({ u: { [stepKey]: 1 }, t: [stepKey] });
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);
    await expect(ctx.sleep(5000)).resolves.toBeUndefined();
  });

  it('re-parks with original wakeUpAt when job wakes up before timer elapses', async () => {
    const futureWakeUpAt = Date.now() + 60_000;
    const stepKey = '0:__sleep__';
    const job = makeJob({ sl: { [stepKey]: futureWakeUpAt }, t: [stepKey] });
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);

    let error!: WorkflowSleepRequestedError;
    try {
      await ctx.sleep(5000);
    } catch (e) {
      error = e as WorkflowSleepRequestedError;
    }

    expect(error).toBeInstanceOf(WorkflowSleepRequestedError);
    expect(error.wakeUpAt).toBe(futureWakeUpAt);
  });
});

describe('WorkflowExecutionContext.waitForSignal()', () => {
  it('throws WorkflowSignalWaitRequestedError on first call', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);
    await expect(ctx.waitForSignal('payment')).rejects.toBeInstanceOf(
      WorkflowSignalWaitRequestedError,
    );
  });

  it('sets expiresAt when timeoutMs provided', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);
    const before = Date.now();
    let error!: WorkflowSignalWaitRequestedError;
    try {
      await ctx.waitForSignal('payment', { timeoutMs: 3600_000 });
    } catch (e) {
      error = e as WorkflowSignalWaitRequestedError;
    }
    expect(error.expiresAt).toBeGreaterThanOrEqual(before + 3599_000);
  });

  it('returns null when timeout has passed', async () => {
    const stepKey = '0:__signal__:payment';
    const job = makeJob({
      ps: { payment: { k: stepKey, e: Date.now() - 1000 } },
      t: [stepKey],
    });
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);
    const result = await ctx.waitForSignal('payment', { timeoutMs: 3600_000 });
    expect(result).toBeNull();
  });

  it('returns signal payload on replay when signal was delivered', async () => {
    const stepKey = '0:__signal__:payment';
    const payload = { amount: 42 };
    const job = makeJob({
      c: { [stepKey]: payload },
      t: [stepKey],
    });
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);
    const result = await ctx.waitForSignal<{ amount: number }>('payment');
    expect(result).toEqual(payload);
  });

  it('re-parks with undefined expiresAt when pending signal has no timeout', async () => {
    const stepKey = '0:__signal__:event';
    const job = makeJob({
      ps: { event: { k: stepKey } },
      t: [stepKey],
    });
    const store = new WorkflowStateStore(job);
    const ctx = new WorkflowExecutionContext(store);

    let error!: WorkflowSignalWaitRequestedError;
    try {
      await ctx.waitForSignal('event');
    } catch (e) {
      error = e as WorkflowSignalWaitRequestedError;
    }

    expect(error).toBeInstanceOf(WorkflowSignalWaitRequestedError);
    expect(error.expiresAt).toBeUndefined();
  });
});
