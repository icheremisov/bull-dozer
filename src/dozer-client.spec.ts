import { Test } from '@nestjs/testing';
import {
  createWorkflowResultProcessor,
  decodeWorkflowResultJob,
  DOZER_JOB_INPUT_KEY,
  DOZER_JOB_STATE_KEY,
  DozerClient,
  DozerModule,
  InMemoryWorkflowQueue,
  toWorkflowResultQueueJobId,
  WORKFLOW_STATUS,
} from './index';
import { DuplicateJobIdResultQueue } from './test/workflow-test-utils';

describe('DozerClient module', () => {
  it('starts workflows from client-only module without worker providers', async () => {
    const queue = new InMemoryWorkflowQueue();
    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forClient({
          driver: queue,
          defaults: {
            job: {
              attempts: 3,
            },
          },
        }),
      ],
    }).compile();

    await moduleRef.init();

    try {
      const client = moduleRef.get(DozerClient);
      const jobId = await client.start(
        'external-workflow',
        { orderId: 42 },
        { removeOnComplete: true },
      );
      const job = await queue.get(jobId);

      expect(job).toBeDefined();
      expect(job?.name).toBe('external-workflow');
      expect(job?.data[DOZER_JOB_INPUT_KEY]).toEqual({ orderId: 42 });
      expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.pending);
      expect(job?.options).toEqual({
        attempts: 3,
        removeOnComplete: true,
      });

      await expect(client.getJobInfo(jobId)).resolves.toMatchObject({
        id: jobId,
        name: 'external-workflow',
        status: WORKFLOW_STATUS.pending,
        statusName: 'pending',
      });
      await expect(client.cancel(jobId)).resolves.toBe(true);
      await expect(client.getJobInfo(jobId)).resolves.toMatchObject({
        id: jobId,
        status: WORKFLOW_STATUS.cancelled,
        statusName: 'cancelled',
      });
    } finally {
      await moduleRef.close();
    }
  });

  it('reads and waits for workflow results from configured result queue', async () => {
    const queue = new InMemoryWorkflowQueue();
    const resultQueue = new DuplicateJobIdResultQueue();
    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forClient({
          driver: queue,
          resultQueue,
        }),
      ],
    }).compile();

    await moduleRef.init();

    try {
      const client = moduleRef.get(DozerClient);
      const jobId = await client.start('external-result-workflow', {
        id: 1,
      });

      await expect(client.hasResult(jobId)).resolves.toBe(false);
      await expect(client.getResult(jobId)).resolves.toBeNull();

      const resultQueueJobId = toWorkflowResultQueueJobId(jobId);
      await resultQueue.add(
        'workflow-result',
        {
          jobId,
          workflowName: 'external-result-workflow',
          result: {
            __dozer_serialized__: 'date',
            v: '2026-02-25T00:00:00.000Z',
          },
          status: 'completed',
        },
        { jobId: resultQueueJobId },
      );

      await expect(client.hasResult(jobId)).resolves.toBe(true);
      await expect(client.getResultJob<Date>(jobId)).resolves.toMatchObject({
        id: resultQueueJobId,
        name: 'workflow-result',
        jobId,
        workflowName: 'external-result-workflow',
      });
      await expect(client.getResult<Date>(jobId)).resolves.toBeInstanceOf(Date);
      await expect(client.waitForResult<Date>(jobId)).resolves.toBeInstanceOf(
        Date,
      );
    } finally {
      await moduleRef.close();
    }
  });

  it('fails fast in waitForResult when workflow reaches failed status without result payload', async () => {
    const queue = new InMemoryWorkflowQueue();
    const resultQueue = new DuplicateJobIdResultQueue();
    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forClient({
          driver: queue,
          resultQueue,
        }),
      ],
    }).compile();

    await moduleRef.init();

    try {
      const client = moduleRef.get(DozerClient);
      const jobId = await client.start('external-result-workflow', {
        id: 2,
      });
      const job = await queue.get(jobId);
      await job?.updateData({
        ...job.data,
        [DOZER_JOB_STATE_KEY]: {
          ...(job.data[DOZER_JOB_STATE_KEY] ?? { c: {}, a: {}, t: [] }),
          s: WORKFLOW_STATUS.failed,
          e: 'failed-for-test',
        },
      });

      await expect(
        client.waitForResult(jobId, { timeoutMs: 50, pollMs: 5 }),
      ).rejects.toThrow('finished with status "failed"');
    } finally {
      await moduleRef.close();
    }
  });

  it('decodes result queue job payload and deserializes result for handler wrapper', async () => {
    const rawJob = {
      id: '$123',
      name: 'workflow-result',
      data: {
        jobId: '123',
        workflowName: 'my-workflow',
        status: 'completed',
        result: {
          __dozer_serialized__: 'date',
          v: '2026-02-25T00:00:00.000Z',
        },
      },
    } as unknown as Parameters<typeof decodeWorkflowResultJob<Date>>[0];

    const decoded = decodeWorkflowResultJob<Date>(rawJob);
    expect(decoded).toMatchObject({
      resultJobId: '$123',
      resultJobName: 'workflow-result',
      workflowJobId: '123',
      workflowName: 'my-workflow',
      status: 'completed',
    });
    expect(decoded.result).toBeInstanceOf(Date);

    const handler = jest.fn<Promise<string>, [typeof decoded, typeof rawJob]>(
      (message) => {
        expect(message.result).toBeInstanceOf(Date);
        return Promise.resolve('ok');
      },
    );
    const processor = createWorkflowResultProcessor<Date, string>(handler);
    await expect(processor(rawJob as never)).resolves.toBe('ok');
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('requires real BullMQ Queue instance for createResultWorker convenience method', async () => {
    const queue = new InMemoryWorkflowQueue();
    const resultQueue = new DuplicateJobIdResultQueue();
    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forClient({
          driver: queue,
          resultQueue,
        }),
      ],
    }).compile();

    await moduleRef.init();

    try {
      const client = moduleRef.get(DozerClient);
      expect(() => client.createResultWorker(() => undefined)).toThrow(
        'requires a real BullMQ Queue instance',
      );
    } finally {
      await moduleRef.close();
    }
  });

  describe('DozerClient.sendSignal()', () => {
    it('returns false when no pending signal exists', async () => {
      const queue = new InMemoryWorkflowQueue();
      const localModule = await Test.createTestingModule({
        imports: [DozerModule.forRoot({ driver: queue })],
      }).compile();
      await localModule.init();
      const client = localModule.get(DozerClient);

      const jobId = await queue.add('any-workflow', {
        __dozer_input__: {},
        __dozer_state__: { s: WORKFLOW_STATUS.running, c: {}, t: [] },
      }).then((j) => j.id);

      const result = await client.sendSignal(jobId, 'payment', { amount: 100 });
      expect(result).toBe(false);

      await localModule.close();
    });

    it('throws WorkflowJobNotFoundError for unknown jobId', async () => {
      const queue = new InMemoryWorkflowQueue();
      const localModule = await Test.createTestingModule({
        imports: [DozerModule.forRoot({ driver: queue })],
      }).compile();
      await localModule.init();
      const client = localModule.get(DozerClient);

      await expect(client.sendSignal('nonexistent', 'event')).rejects.toThrow(
        'not found',
      );

      await localModule.close();
    });

    it('delivers signal with undefined payload when no payload argument given', async () => {
      const queue = new InMemoryWorkflowQueue();
      const localModule = await Test.createTestingModule({
        imports: [DozerModule.forRoot({ driver: queue })],
      }).compile();
      await localModule.init();
      const client = localModule.get(DozerClient);

      const jobId = await queue.add('any-workflow', {
        [DOZER_JOB_INPUT_KEY]: {},
        [DOZER_JOB_STATE_KEY]: {
          s: WORKFLOW_STATUS.running,
          c: {},
          t: ['0:__signal__:event'],
          ps: { event: { k: '0:__signal__:event' } },
        },
      }).then((j) => j.id);

      const result = await client.sendSignal(jobId, 'event');
      expect(result).toBe(true);

      const job = await queue.get(jobId);
      const state = job!.data[DOZER_JOB_STATE_KEY];
      expect('0:__signal__:event' in (state?.c ?? {})).toBe(true);
      expect(state?.ps).toBeUndefined();
    });
  });
});
