import { DOZER_JOB_INPUT_KEY, DOZER_JOB_STATE_KEY } from '../constants';
import { WORKFLOW_STATUS } from './workflow-queue';
import { InMemoryWorkflowQueue } from './in-memory-workflow-queue';

const makeJobData = () => ({
  [DOZER_JOB_INPUT_KEY]: {},
  [DOZER_JOB_STATE_KEY]: { s: WORKFLOW_STATUS.pending, c: {}, t: [] },
});

describe('InMemoryWorkflowQueue delayed jobs', () => {
  it('moveToDelayed stores the job as delayed', async () => {
    const queue = new InMemoryWorkflowQueue();
    const job = await queue.add('test', makeJobData());
    await queue.moveToDelayed(job.id, Date.now() + 10_000);
    expect(queue.isDelayed(job.id)).toBe(true);
  });

  it('promoteDelayed makes job no longer delayed', async () => {
    const queue = new InMemoryWorkflowQueue();
    const job = await queue.add('test', makeJobData());
    await queue.moveToDelayed(job.id, Date.now() + 10_000);
    await queue.promoteDelayed(job.id);
    expect(queue.isDelayed(job.id)).toBe(false);
  });

  it('get() still returns a delayed job', async () => {
    const queue = new InMemoryWorkflowQueue();
    const job = await queue.add('test', makeJobData());
    await queue.moveToDelayed(job.id, Date.now() + 10_000);
    const fetched = await queue.get(job.id);
    expect(fetched).not.toBeNull();
    expect(fetched?.id).toBe(job.id);
  });

  it('moveToDelayed is a no-op for unknown jobId', async () => {
    const queue = new InMemoryWorkflowQueue();
    await expect(
      queue.moveToDelayed('unknown', Date.now() + 1000),
    ).resolves.toBeUndefined();
  });

  it('promoteDelayed is a no-op for unknown jobId', async () => {
    const queue = new InMemoryWorkflowQueue();
    await expect(queue.promoteDelayed('unknown')).resolves.toBeUndefined();
  });
});
