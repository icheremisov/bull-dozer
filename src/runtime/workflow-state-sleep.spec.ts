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

describe('WorkflowStateStore sleep methods', () => {
  it('saveSleepIntent saves wakeUpAt under stepKey in sl', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    await store.saveSleepIntent('0.0:__sleep__', 9999999);
    expect(job.data[DOZER_JOB_STATE_KEY]?.sl?.['0.0:__sleep__']).toBe(9999999);
  });

  it('getSleepIntent returns the saved wakeUpAt', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    await store.saveSleepIntent('0.0:__sleep__', 1234567);
    expect(store.getSleepIntent('0.0:__sleep__')).toBe(1234567);
  });

  it('getSleepIntent returns undefined for unknown key', () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    expect(store.getSleepIntent('missing')).toBeUndefined();
  });

  it('completeSleep moves stepKey from sl to u and removes it from sl', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    await store.saveSleepIntent('0.0:__sleep__', 9999999);
    await store.completeSleep('0.0:__sleep__');
    const state = job.data[DOZER_JOB_STATE_KEY]!;
    expect(state.sl?.['0.0:__sleep__']).toBeUndefined();
    expect(state.u?.['0.0:__sleep__']).toBe(1);
  });

  it('completeSleep removes sl field entirely when last entry is removed', async () => {
    const job = makeJob();
    const store = new WorkflowStateStore(job);
    await store.saveSleepIntent('0:__sleep__', 9999999);
    await store.completeSleep('0:__sleep__');
    const state = job.data[DOZER_JOB_STATE_KEY]!;
    expect(state.sl).toBeUndefined();
  });
});
