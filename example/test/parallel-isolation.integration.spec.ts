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
import {
  EXAMPLE_RESULT_QUEUE,
  EXAMPLE_WORKFLOW_QUEUE,
} from '../src/infra/tokens';
import { FailureMemoryService } from '../src/support/failure-memory.service';
import { isRedisReachable, redisTestConfig } from './helpers/redis';

jest.setTimeout(120_000);

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

const waitForTerminalStatus = async (
  queue: Queue<WorkflowJobData<unknown>>,
  jobId: string,
  timeoutMs = 30_000,
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
  const lastCompactState = lastJob?.data?.[DOZER_JOB_STATE_KEY];
  const failedReason =
    lastJob && 'failedReason' in lastJob
      ? String((lastJob as unknown as { failedReason?: unknown }).failedReason)
      : undefined;

  throw new Error(
    `Timed out waiting for job ${jobId} to reach terminal status. queueState=${String(lastQueueState)} compactState=${JSON.stringify(lastCompactState)} failedReason=${failedReason ?? 'n/a'}`,
  );
};

function computeExpected(
  seed: number,
  coeffA: number,
  coeffB: number,
  steps: number,
): number {
  const MOD = 1_000_000_007;
  let value = seed;
  for (let i = 0; i < steps; i++) {
    value = (value * coeffA + coeffB) % MOD;
  }
  return value;
}

describe('Parallel stress / isolation integration', () => {
  let moduleRef: TestingModule;
  let app: INestApplication;
  let queue: Queue<WorkflowJobData<unknown>>;
  let resultQueue: Queue<WorkflowResultQueueJobData<unknown>>;
  let engine: DozerEngine;
  let failureMemory: FailureMemoryService;
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

      process.stderr.write(
        `${message} Parallel isolation integration tests are skipped.\n`,
      );
      return;
    }

    moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleRef.createNestApplication();
    app.useLogger(false);
    await app.init();

    queue = app.get<Queue<WorkflowJobData<unknown>>>(EXAMPLE_WORKFLOW_QUEUE);
    resultQueue =
      app.get<Queue<WorkflowResultQueueJobData<unknown>>>(EXAMPLE_RESULT_QUEUE);
    engine = app.get(DozerEngine);
    failureMemory = app.get(FailureMemoryService);
  }, 30000);

  beforeEach(() => {
    if (!redisAvailable) {
      return;
    }

    failureMemory.reset();
  });

  afterAll(async () => {
    if (!redisAvailable) {
      return;
    }

    await app.close();
    await queue.close();
    await resultQueue.close();
  }, 20000);

  integrationTest(
    '20 parallel workflows complete with isolated results',
    async () => {
      const STEPS = 20;
      const cases = Array.from({ length: 20 }, (_, i) => ({
        id: `iso-${i}-${Date.now()}`,
        seed: (i + 1) * 13,
        coeffA: (i % 7) + 2,
        coeffB: (i % 5) + 1,
        steps: STEPS,
      }));

      const jobIds = await Promise.all(
        cases.map((c) => engine.start('parallel-stress', c)),
      );

      const results = await Promise.all(
        jobIds.map((jobId) => waitForTerminalStatus(queue, jobId, 60_000)),
      );

      for (let i = 0; i < cases.length; i++) {
        const input = cases[i];
        const data = results[i];
        const result = data[DOZER_JOB_STATE_KEY]?.r as
          | { id: string; value: number; steps: number }
          | undefined;

        expect(result).toBeDefined();
        expect(result!.id).toBe(input.id);
        expect(result!.value).toBe(
          computeExpected(input.seed, input.coeffA, input.coeffB, input.steps),
        );
        expect(result!.steps).toBe(STEPS);

        const jobId = jobIds[i];
        const logsResult = await queue.getJobLogs(jobId);
        const logs = logsResult.logs;

        expect(logs.length).toBeGreaterThanOrEqual(STEPS + 2);
        expect(logs.some((l) => l.startsWith('start id='))).toBe(true);
        expect(logs.some((l) => l.startsWith('done id='))).toBe(true);
      }
    },
  );

  integrationTest(
    '20 parallel workflows with mixed failures and sleeps complete with correct isolated results',
    async () => {
      const STEPS = 15;
      const cases = Array.from({ length: 20 }, (_, i) => {
        const base = {
          id: `mix-${i}-${Date.now()}`,
          seed: (i + 1) * 17,
          coeffA: (i % 7) + 2,
          coeffB: (i % 5) + 1,
          steps: STEPS,
        };

        if (i % 4 === 0) {
          // plain
          return base;
        } else if (i % 4 === 1) {
          return { ...base, failAtStep: 5 };
        } else if (i % 4 === 2) {
          return { ...base, sleepAfterStep: 3, sleepMs: 150 };
        } else {
          return { ...base, failAtStep: 8, sleepAfterStep: 5, sleepMs: 150 };
        }
      });

      const jobIds = await Promise.all(
        cases.map((c) => engine.start('parallel-stress', c)),
      );

      const results = await Promise.all(
        jobIds.map((jobId) => waitForTerminalStatus(queue, jobId, 90_000)),
      );

      const seenIds = new Set<string>();

      for (let i = 0; i < cases.length; i++) {
        const input = cases[i];
        const data = results[i];
        const compactState = data[DOZER_JOB_STATE_KEY];

        expect(compactState?.s).toBe(WORKFLOW_STATUS.completed);

        const result = compactState?.r as
          | { id: string; value: number; steps: number }
          | undefined;

        expect(result).toBeDefined();
        expect(result!.value).toBe(
          computeExpected(input.seed, input.coeffA, input.coeffB, input.steps),
        );

        seenIds.add(result!.id);
      }

      // All 20 IDs are distinct
      expect(seenIds.size).toBe(20);
    },
  );

  integrationTest(
    "job isolation: each job's logs contain only its own ID",
    async () => {
      const STEPS = 10;
      const cases = Array.from({ length: 10 }, (_, i) => ({
        id: `log-iso-${i}-${Date.now()}`,
        seed: (i + 1) * 7,
        coeffA: (i % 7) + 2,
        coeffB: (i % 5) + 1,
        steps: STEPS,
      }));

      const jobIds = await Promise.all(
        cases.map((c) => engine.start('parallel-stress', c)),
      );

      await Promise.all(
        jobIds.map((jobId) => waitForTerminalStatus(queue, jobId, 60_000)),
      );

      for (let i = 0; i < cases.length; i++) {
        const otherIds = cases.filter((_, j) => j !== i).map((c) => c.id);

        const logsResult = await queue.getJobLogs(jobIds[i]);
        const logs = logsResult.logs;

        for (const log of logs) {
          for (const otherId of otherIds) {
            expect(log).not.toContain(otherId);
          }
        }
      }
    },
  );
});
