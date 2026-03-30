import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Queue } from 'bullmq';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  WORKFLOW_STATUS,
  WorkflowJobData,
  WorkflowResultQueueJobData,
} from 'dozer';
import { AppModule } from '../src/app.module';
import { setupBullBoard } from '../src/infra/bull-board';
import {
  EXAMPLE_RESULT_QUEUE,
  EXAMPLE_WORKFLOW_QUEUE,
} from '../src/infra/tokens';
import { isRedisReachable, redisTestConfig } from './helpers/redis';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

const waitForTerminalStatus = async (
  queue: Queue<WorkflowJobData<unknown>>,
  jobId: string,
  timeoutMs = 15000,
): Promise<WorkflowJobData<unknown>> => {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const job = await queue.getJob(jobId);
    if (job) {
      const state = job.data[DOZER_JOB_STATE_KEY];
      const queueState = await job.getState();
      if (
        state &&
        (state.s === WORKFLOW_STATUS.completed ||
          state.s === WORKFLOW_STATUS.failed) &&
        (queueState === 'completed' || queueState === 'failed')
      ) {
        return job.data;
      }
    }

    await sleep(100);
  }

  const lastJob = await queue.getJob(jobId);
  const lastQueueState = lastJob ? await lastJob.getState() : 'missing';
  throw new Error(
    `Timed out waiting for job ${jobId} to reach terminal status. queueState=${String(lastQueueState)}`,
  );
};

jest.setTimeout(60000);

describe('Job runtime methods integration (real Redis + BullMQ)', () => {
  let moduleRef: TestingModule;
  let app: INestApplication;
  let queue: Queue<WorkflowJobData<unknown>>;
  let resultQueue: Queue<WorkflowResultQueueJobData<unknown>>;
  let engine: DozerEngine;
  let redisAvailable = false;

  const integrationTest = (name: string, fn: () => Promise<void>): void => {
    it(name, async () => {
      if (!redisAvailable) {
        return;
      }

      await fn();
    });
  };

  beforeAll(async () => {
    redisAvailable = await isRedisReachable();
    if (!redisAvailable) {
      const message = `Redis is not reachable at ${redisTestConfig.target}.`;
      if (redisTestConfig.required) {
        throw new Error(
          `${message} Set REDIS_HOST/REDIS_PORT or REDIS_CLUSTER_NODES/REDIS_MODE correctly.`,
        );
      }

      process.stderr.write(`${message} Integration tests are skipped.\n`);
      return;
    }

    moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleRef.createNestApplication();
    app.useLogger(false);
    queue = app.get<Queue<WorkflowJobData<unknown>>>(EXAMPLE_WORKFLOW_QUEUE);
    resultQueue =
      app.get<Queue<WorkflowResultQueueJobData<unknown>>>(EXAMPLE_RESULT_QUEUE);
    setupBullBoard(app, [queue, resultQueue]);
    engine = moduleRef.get(DozerEngine);
    await app.init();
  }, 30000);

  afterAll(async () => {
    if (!redisAvailable) {
      return;
    }

    await app.close();
    await queue.close();
    await resultQueue.close();
  }, 20000);

  integrationTest(
    'log(): appended entries are visible via queue.getJobLogs()',
    async () => {
      const jobId = await engine.start('job-runtime', { steps: 1 });
      await waitForTerminalStatus(queue, jobId);

      const { logs, count } = await queue.getJobLogs(jobId);

      expect(count).toBeGreaterThanOrEqual(3);
      expect(logs).toContain('workflow started');
      expect(logs.some((l) => l.startsWith('step-a done:'))).toBe(true);
      expect(logs.some((l) => l.startsWith('step-b done:'))).toBe(true);
    },
  );

  integrationTest(
    'updateProgress(): final progress is 100 after workflow completes',
    async () => {
      const jobId = await engine.start('job-runtime', { steps: 2 });
      await waitForTerminalStatus(queue, jobId);

      const job = await queue.getJob(jobId);
      expect(job).toBeDefined();
      expect(job?.progress).toBe(100);
    },
  );

  integrationTest(
    'clearLogs(): wipes all entries when called mid-run',
    async () => {
      const jobId = await engine.start('job-runtime', {
        steps: 3,
        clearAfterFirstStep: true,
      });
      await waitForTerminalStatus(queue, jobId);

      const { logs, count } = await queue.getJobLogs(jobId);

      // Only step-b log should remain; the earlier two were cleared
      expect(count).toBe(1);
      expect(logs[0]).toMatch(/^step-b done:/);
    },
  );

  integrationTest(
    'changePriority(): job priority is updated in BullMQ',
    async () => {
      // Start with default priority 0, then change to 5 mid-run
      const jobId = await engine.start('job-runtime', {
        steps: 4,
        priorityChange: 5,
      });
      await waitForTerminalStatus(queue, jobId);

      const job = await queue.getJob(jobId);
      expect(job).toBeDefined();
      // BullMQ stores priority on the completed job; value after changePriority should be 5
      expect(job?.priority).toBe(5);
    },
  );

  integrationTest(
    'getJobLogs() returns empty result for unknown job id',
    async () => {
      const { logs, count } = await queue.getJobLogs('non-existent-job-id');
      expect(logs).toEqual([]);
      expect(count).toBe(0);
    },
  );
});
