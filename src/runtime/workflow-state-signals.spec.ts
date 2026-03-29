import { DOZER_JOB_INPUT_KEY, DOZER_JOB_STATE_KEY } from '../constants';
import { WORKFLOW_STATUS } from '../queue/workflow-queue';
import type { WorkflowJob, WorkflowJobData } from '../queue/workflow-queue';
import { WorkflowStateStore } from './workflow-state.store';

const makeJob = (): WorkflowJob<unknown> => {
  let data: WorkflowJobData<unknown> = {
    [DOZER_JOB_INPUT_KEY]: {},
    [DOZER_JOB_STATE_KEY]: {
      s: WORKFLOW_STATUS.running,
      c: {},
      t: [],
    },
  };
  return {
    id: 'test-job',
    name: 'test',
    get data() {
      return data;
    },
    updateData: (next: WorkflowJobData<unknown>): Promise<void> => {
      data = next;
      return Promise.resolve();
    },
  };
};

describe('WorkflowStateStore signal methods', () => {
  it('savePendingSignal saves signal entry in ps', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    await store.savePendingSignal('payment', '0.1:__signal__:payment', 9999999);
    const ps = job.data[DOZER_JOB_STATE_KEY]?.ps;
    expect(ps?.['payment']).toEqual({
      k: '0.1:__signal__:payment',
      e: 9999999,
    });
  });

  it('savePendingSignal without expiresAt omits e field', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    await store.savePendingSignal('event', '0.2:__signal__:event');
    const ps = job.data[DOZER_JOB_STATE_KEY]?.ps;
    expect(ps?.['event']).toEqual({ k: '0.2:__signal__:event' });
  });

  it('getPendingSignal returns saved entry', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    await store.savePendingSignal('payment', '0.1:__signal__:payment', 1000);
    expect(store.getPendingSignal('payment')).toEqual({
      stepKey: '0.1:__signal__:payment',
      expiresAt: 1000,
    });
  });

  it('getPendingSignal returns undefined for unknown signal', () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    expect(store.getPendingSignal('missing')).toBeUndefined();
  });

  it('clearPendingSignal removes signal from ps', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    await store.savePendingSignal('payment', '0.1:__signal__:payment');
    await store.clearPendingSignal('payment');
    expect(store.getPendingSignal('payment')).toBeUndefined();
    expect(job.data[DOZER_JOB_STATE_KEY]?.ps).toBeUndefined();
  });

  it('deliverSignal saves payload in c, clears ps, returns true', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    job.data[DOZER_JOB_STATE_KEY]!.t = ['0.1:__signal__:payment'];
    await store.savePendingSignal('payment', '0.1:__signal__:payment');
    const delivered = await store.deliverSignal('payment', { amount: 100 });
    expect(delivered).toBe(true);
    expect(store.getPendingSignal('payment')).toBeUndefined();
    const state = job.data[DOZER_JOB_STATE_KEY]!;
    expect('0.1:__signal__:payment' in state.c).toBe(true);
  });

  it('deliverSignal returns false when no pending signal with that name', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    const delivered = await store.deliverSignal('not-registered', {});
    expect(delivered).toBe(false);
  });
});
