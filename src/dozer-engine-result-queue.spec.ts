import { Test } from '@nestjs/testing';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  Step,
  toWorkflowResultQueueJobId,
  Workflow,
  WORKFLOW_STATUS,
} from './index';
import {
  CapturingResultQueue,
  DuplicateJobIdResultQueue,
  FailOnceResultQueue,
} from './test/workflow-test-utils';
import { DozerWorkflow } from './workflow/dozer-workflow';

@Workflow({
  name: 'result-queue-workflow',
  resultQueue: {
    jobName: 'workflow-result',
    job: {
      removeOnComplete: true,
    },
  },
})
class ResultQueueWorkflow extends DozerWorkflow<{ value: number }> {
  run(input: { value: number }): Promise<{ value: number }> {
    return Promise.resolve({ value: input.value + 1 });
  }
}

describe('DozerEngine result queue', () => {
  it('publishes completed workflow result to configured result queue', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const resultQueue = new CapturingResultQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          resultQueue,
        }),
        DozerModule.forFeature([ResultQueueWorkflow]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('result-queue-workflow', {
        value: 10,
      });

      await expect(localEngine.run(jobId)).resolves.toEqual({ value: 11 });

      expect(resultQueue.added).toHaveLength(1);
      expect(resultQueue.added[0]).toMatchObject({
        name: 'workflow-result',
        data: {
          jobId,
          workflowName: 'result-queue-workflow',
          status: 'completed',
          result: { value: 11 },
        },
        options: {
          jobId,
          removeOnComplete: true,
        },
      });
    } finally {
      await localModule.close();
    }
  });

  it('keeps workflow in completing status when result queue publish fails and resumes finalize later', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const resultQueue = new FailOnceResultQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          resultQueue,
        }),
        DozerModule.forFeature([ResultQueueWorkflow]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('result-queue-workflow', {
        value: 20,
      });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'result-queue-temporary-failure',
      );
      await expect(localEngine.getJobInfo(jobId)).resolves.toMatchObject({
        id: jobId,
        status: WORKFLOW_STATUS.completing,
        statusName: 'completing',
        result: { value: 21 },
      });

      await expect(localEngine.run(jobId)).resolves.toEqual({ value: 21 });
      await expect(localEngine.getJobInfo(jobId)).resolves.toMatchObject({
        id: jobId,
        status: WORKFLOW_STATUS.completed,
        statusName: 'completed',
        result: { value: 21 },
      });
      expect(resultQueue.added).toHaveLength(1);
      expect(resultQueue.added[0]?.options).toMatchObject({ jobId });
    } finally {
      await localModule.close();
    }
  });

  it('resumes completing workflow when result job already exists without creating duplicate', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const resultQueue = new DuplicateJobIdResultQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          resultQueue,
        }),
        DozerModule.forFeature([ResultQueueWorkflow]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('result-queue-workflow', {
        value: 30,
      });
      const persistedResult = { value: 31 };

      await resultQueue.add(
        'workflow-result',
        {
          jobId,
          workflowName: 'result-queue-workflow',
          result: persistedResult,
          status: 'completed',
        },
        { jobId },
      );

      const workflowJob = await localQueue.get(jobId);
      await workflowJob?.updateData({
        ...workflowJob?.data,
        [DOZER_JOB_STATE_KEY]: {
          s: WORKFLOW_STATUS.completing,
          c: {},
          a: {},
          t: [],
          r: persistedResult,
        },
      });

      await expect(localEngine.run(jobId)).resolves.toEqual(persistedResult);
      await expect(localEngine.getJobInfo(jobId)).resolves.toMatchObject({
        id: jobId,
        status: WORKFLOW_STATUS.completed,
        statusName: 'completed',
        result: persistedResult,
      });
      expect(resultQueue.added).toHaveLength(1);
      expect(resultQueue.added[0]?.options).toMatchObject({ jobId });
    } finally {
      await localModule.close();
    }
  });
});
