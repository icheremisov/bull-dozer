import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Queue } from 'bullmq';
import {
  DozerClient,
  DozerEngine,
  DOZER_JOB_STATE_KEY,
  WORKFLOW_STATUS,
  WorkflowJobData,
  WorkflowResultQueueJobData,
} from 'dozer';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { setupBullBoard } from '../src/infra/bull-board';
import {
  EXAMPLE_RESULT_QUEUE,
  EXAMPLE_WORKFLOW_QUEUE,
} from '../src/infra/tokens';
import { BranchSelectorService } from '../src/support/branch-selector.service';
import { FailureMemoryService } from '../src/support/failure-memory.service';
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
  const lastCompactState = lastJob?.data?.[DOZER_JOB_STATE_KEY];
  const failedReason =
    lastJob && 'failedReason' in lastJob
      ? String((lastJob as unknown as { failedReason?: unknown }).failedReason)
      : undefined;

  throw new Error(
    `Timed out waiting for job ${jobId} to reach terminal status. queueState=${String(lastQueueState)} compactState=${JSON.stringify(lastCompactState)} failedReason=${failedReason ?? 'n/a'}`,
  );
};

const waitForDelayed = async (
  queue: Queue<WorkflowJobData<unknown>>,
  jobId: string,
  timeoutMs = 5000,
): Promise<void> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = await queue.getJob(jobId);
    if (job) {
      const state = await job.getState();
      if (state === 'delayed') return;
    }
    await sleep(100);
  }
  throw new Error(`Timed out waiting for job ${jobId} to reach delayed state`);
};

jest.setTimeout(60000);

describe('Example workflows integration (real Redis + BullMQ)', () => {
  let moduleRef: TestingModule;
  let app: INestApplication;
  let queue: Queue<WorkflowJobData<unknown>>;
  let resultQueue: Queue<WorkflowResultQueueJobData<unknown>>;
  let branchSelector: BranchSelectorService;
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
    await app.init();
    branchSelector = app.get(BranchSelectorService);
    failureMemory = app.get(FailureMemoryService);
  }, 30000);

  beforeEach(() => {
    if (!redisAvailable) {
      return;
    }

    branchSelector.reset();
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

  integrationTest('runs simple workflow and stores compact state', async () => {
    const start = await request(app.getHttpServer())
      .post('/workflows/simple/start')
      .send({ orderId: 42 })
      .expect(201);

    const { jobId } = start.body as { jobId: string };
    const data = await waitForTerminalStatus(queue, jobId);

    const compactState = data[DOZER_JOB_STATE_KEY];
    expect(compactState?.s).toBe(WORKFLOW_STATUS.completed);
    expect(compactState?.c['0:validate']).toBeDefined();
    expect(compactState?.c['1:process']).toBeDefined();
    expect(compactState?.c['2:store']).toBeDefined();
  });

  integrationTest(
    'publishes workflow result into separate result queue with deterministic jobId',
    async () => {
      const start = await request(app.getHttpServer())
        .post('/workflows/result-queue/start')
        .send({ value: 10 })
        .expect(201);

      const { jobId } = start.body as { jobId: string };
      const data = await waitForTerminalStatus(queue, jobId);
      expect(data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);

      const resultQueueJobId = toResultQueueJobId(jobId);
      const resultJob = await resultQueue.getJob(resultQueueJobId);
      expect(resultJob).toBeDefined();
      expect(String(resultJob?.id)).toBe(resultQueueJobId);
      expect(resultJob?.name).toBe('workflow-result');
      expect(resultJob?.data).toEqual({
        jobId,
        workflowName: 'result-queue',
        status: 'completed',
        result: {
          ok: true,
          value: 11,
        },
      });
    },
  );

  integrationTest('exposes result queue job via HTTP endpoint', async () => {
    const start = await request(app.getHttpServer())
      .post('/workflows/result-queue/start')
      .send({ value: 41 })
      .expect(201);

    const { jobId } = start.body as { jobId: string };
    await waitForTerminalStatus(queue, jobId);

    const response = await request(app.getHttpServer())
      .get(`/workflows/results/${jobId}`)
      .expect(200);

    expect(response.body).toMatchObject({
      found: true,
      id: toResultQueueJobId(jobId),
      name: 'workflow-result',
      data: {
        jobId,
        workflowName: 'result-queue',
        result: {
          ok: true,
          value: 42,
        },
      },
    });
  });

  integrationTest(
    'resumes completing workflow when result queue job already exists without duplicate',
    async () => {
      const start = await request(app.getHttpServer())
        .post('/workflows/result-queue/start')
        .send({ value: 50 })
        .expect(201);

      const { jobId } = start.body as { jobId: string };
      const completedData = await waitForTerminalStatus(queue, jobId);
      expect(completedData[DOZER_JOB_STATE_KEY]?.s).toBe(
        WORKFLOW_STATUS.completed,
      );

      const resultQueueJobId = toResultQueueJobId(jobId);
      const existingResultJob = await resultQueue.getJob(resultQueueJobId);
      expect(existingResultJob).toBeDefined();

      const workflowJob = await queue.getJob(jobId);
      expect(workflowJob).toBeDefined();
      const compactState = workflowJob?.data[DOZER_JOB_STATE_KEY];
      expect(compactState?.r).toEqual({
        ok: true,
        value: 51,
      });

      await workflowJob?.updateData({
        ...workflowJob?.data,
        [DOZER_JOB_STATE_KEY]: {
          ...(compactState ?? { c: {}, t: [] }),
          s: WORKFLOW_STATUS.completing,
        },
      });

      const resultQueueCountBeforeReplay = await resultQueue.count();

      await request(app.getHttpServer())
        .post(`/workflows/${jobId}/replay`)
        .send({})
        .expect(201)
        .expect(({ body }) => {
          expect(body.result).toEqual({
            ok: true,
            value: 51,
          });
        });

      const replayedData = await waitForTerminalStatus(queue, jobId);
      expect(replayedData[DOZER_JOB_STATE_KEY]?.s).toBe(
        WORKFLOW_STATUS.completed,
      );
      expect(replayedData[DOZER_JOB_STATE_KEY]?.r).toEqual({
        ok: true,
        value: 51,
      });

      const resultQueueCountAfterReplay = await resultQueue.count();
      expect(resultQueueCountAfterReplay).toBe(resultQueueCountBeforeReplay);

      const resultJob = await resultQueue.getJob(resultQueueJobId);
      expect(resultJob).toBeDefined();
      expect(resultJob?.data).toEqual(existingResultJob?.data);
    },
  );

  integrationTest('supports various workflow input types', async () => {
    const cases = [
      { kind: 'number', value: 7, expected: '7' },
      { kind: 'string', value: 'abc', expected: 'abc' },
      { kind: 'object', value: { id: 1, tag: 'x' }, expected: '1:x' },
      { kind: 'array', value: ['a', 'b'], expected: 'a,b' },
      { kind: 'null', value: null, expected: 'null' },
    ];

    for (const testCase of cases) {
      const response = await request(app.getHttpServer())
        .post('/workflows/typed-input/start')
        .send(testCase)
        .expect(201);

      const data = await waitForTerminalStatus(queue, response.body.jobId);
      const compactState = data[DOZER_JOB_STATE_KEY];
      expect(compactState?.s).toBe(WORKFLOW_STATUS.completed);
      expect(compactState?.r).toEqual({ normalized: testCase.expected });
    }
  });

  integrationTest(
    'persists steps that return void/undefined and typed values',
    async () => {
      const start = await request(app.getHttpServer())
        .post('/workflows/typed-step/start')
        .send({ value: 5 })
        .expect(201);

      const data = await waitForTerminalStatus(queue, start.body.jobId);
      const compactState = data[DOZER_JOB_STATE_KEY];

      expect(compactState?.s).toBe(WORKFLOW_STATUS.completed);
      expect(compactState?.c['2:number-step']).toBe(6);
      expect(compactState?.u?.['0:void-step']).toBe(1);
      expect(compactState?.u?.['1:undefined-step']).toBe(1);
    },
  );

  integrationTest(
    'stores nested step indexes and repeated step calls distinctly',
    async () => {
      const nestedStart = await request(app.getHttpServer())
        .post('/workflows/nested/start')
        .send({ value: 1 })
        .expect(201);

      const nestedData = await waitForTerminalStatus(
        queue,
        nestedStart.body.jobId,
      );
      const nestedState = nestedData[DOZER_JOB_STATE_KEY];

      expect(nestedState?.c['0:outer']).toBe(3);
      expect(nestedState?.c['0.0:inner-a']).toBeUndefined();
      expect(nestedState?.c['0.1:inner-b']).toBeUndefined();
      expect(nestedState?.t).toEqual(['0:outer', '0.0:inner-a', '0.1:inner-b']);

      const repeatedStart = await request(app.getHttpServer())
        .post('/workflows/repeated-step/start')
        .send({ value: 1 })
        .expect(201);

      const repeatedData = await waitForTerminalStatus(
        queue,
        repeatedStart.body.jobId,
      );
      const repeatedState = repeatedData[DOZER_JOB_STATE_KEY];

      expect(repeatedState?.c['0:increment']).toBe(2);
      expect(repeatedState?.c['1:increment']).toBe(3);
    },
  );

  integrationTest(
    'handles deterministic and random failures in flaky workflow',
    async () => {
      const deterministic = await request(app.getHttpServer())
        .post('/workflows/flaky/start')
        .send({ key: 'deterministic', threshold: 0, failTimes: 2 })
        .expect(201);

      const deterministicData = await waitForTerminalStatus(
        queue,
        deterministic.body.jobId,
      );
      expect(deterministicData[DOZER_JOB_STATE_KEY]?.s).toBe(
        WORKFLOW_STATUS.completed,
      );

      const random = await request(app.getHttpServer())
        .post('/workflows/flaky/start')
        .send({ key: 'random', threshold: 0.7 })
        .expect(201);

      const randomData = await waitForTerminalStatus(queue, random.body.jobId);
      const randomStatus = randomData[DOZER_JOB_STATE_KEY]?.s;
      expect([WORKFLOW_STATUS.failed, WORKFLOW_STATUS.completed]).toContain(
        randomStatus,
      );
    },
  );

  integrationTest(
    'replays workflow and restores state without re-running completed step',
    async () => {
      const start = await request(app.getHttpServer())
        .post('/workflows/replay/start')
        .send({ id: 'replay-1', value: 10 })
        .expect(201);

      const firstRun = await waitForTerminalStatus(queue, start.body.jobId);
      expect(firstRun[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.failed);
      expect(firstRun[DOZER_JOB_STATE_KEY]?.c['0:first']).toBeDefined();
      expect(firstRun[DOZER_JOB_STATE_KEY]?.c['1:second']).toBeUndefined();

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const replayed = await waitForTerminalStatus(queue, start.body.jobId);
      expect(replayed[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
      expect(replayed[DOZER_JOB_STATE_KEY]?.c['0:first']).toBeDefined();
      expect(replayed[DOZER_JOB_STATE_KEY]?.c['1:second']).toBeDefined();

      expect(failureMemory.calls('replay:replay-1')).toBe(2);
      expect(failureMemory.calls('replay-first:replay-1')).toBe(1);
    },
  );

  integrationTest(
    'restores workflow state from Redis through status endpoint',
    async () => {
      const start = await request(app.getHttpServer())
        .post('/workflows/replay/start')
        .send({ id: 'restore-1', value: 3 })
        .expect(201);

      await waitForTerminalStatus(queue, start.body.jobId);

      const failedStatus = await request(app.getHttpServer())
        .get(`/workflows/${start.body.jobId}`)
        .expect(200);

      const firstCompactState = failedStatus.body.compactState as
        | {
            s: number;
            c: Record<string, unknown>;
          }
        | undefined;

      expect(failedStatus.body.found).toBe(true);
      expect([WORKFLOW_STATUS.failed, WORKFLOW_STATUS.completed]).toContain(
        firstCompactState?.s,
      );
      expect(firstCompactState?.c['0:first']).toBeDefined();

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const replayed = await request(app.getHttpServer())
        .get(`/workflows/${start.body.jobId}`)
        .expect(200);

      expect(replayed.body.compactState?.s).toBe(WORKFLOW_STATUS.completed);
      expect(replayed.body.compactState?.c['0:first']).toBeDefined();
      expect(replayed.body.compactState?.c['1:second']).toBeDefined();
    },
  );

  integrationTest(
    'supports long-running workflow with fail then replay',
    async () => {
      const start = await request(app.getHttpServer())
        .post('/workflows/long-running/start')
        .send({ id: 'long-1', delayMs: 100, failTimes: 1 })
        .expect(201);

      const failed = await waitForTerminalStatus(
        queue,
        start.body.jobId,
        20000,
      );
      expect(failed[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.failed);

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(
        queue,
        start.body.jobId,
        20000,
      );
      expect(completed[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
    },
  );

  integrationTest('detects non-deterministic replay divergence', async () => {
    branchSelector.setBranch('nondet-1', 'left');

    const start = await request(app.getHttpServer())
      .post('/workflows/non-deterministic/start')
      .send({ id: 'nondet-1', value: 1 })
      .expect(201);

    const failed = await waitForTerminalStatus(queue, start.body.jobId);
    const firstState = failed[DOZER_JOB_STATE_KEY];
    expect([WORKFLOW_STATUS.failed, WORKFLOW_STATUS.completed]).toContain(
      firstState?.s,
    );
    expect(firstState?.c['0:left-branch']).toBeDefined();

    branchSelector.setBranch('nondet-1', 'right');

    const replay = await request(app.getHttpServer())
      .post(`/workflows/${start.body.jobId}/replay`)
      .send({});

    expect(replay.status).toBe(500);

    const replayedJob = await queue.getJob(start.body.jobId);
    const replayedState = replayedJob?.data[DOZER_JOB_STATE_KEY];
    expect(replayedState?.s).toBe(WORKFLOW_STATUS.failed);
    expect(String(replayedState?.e ?? '')).toContain('Step replay conflict');
  });

  integrationTest('exposes BullMQ dashboard endpoint', async () => {
    const response = await request(app.getHttpServer())
      .get('/admin/queues/')
      .expect(200);

    expect(String(response.text)).toContain('Bull Dashboard');
  });

  integrationTest('sleep workflow: completes after sleep period elapses', async () => {
    const engine = moduleRef.get(DozerEngine);
    const jobId = await engine.start('sleep-workflow', {
      id: `sleep-test-${Date.now()}`,
      durationMs: 200,
      value: 5,
    });

    const data = await waitForTerminalStatus(queue, jobId, 15000);
    expect(data[DOZER_JOB_STATE_KEY]?.r).toEqual({ value: 6 });
  });

  integrationTest('signal workflow: delivers signal and workflow completes with payload', async () => {
    const engine = moduleRef.get(DozerEngine);
    const dozerClient = moduleRef.get(DozerClient);
    const id = `signal-test-${Date.now()}`;

    const jobId = await engine.start('signal-workflow', {
      id,
      timeoutMs: 30_000,
    });

    await waitForDelayed(queue, jobId, 10_000);

    const delivered = await dozerClient.sendSignal(jobId, 'approval', { userId: 'user-1' });
    expect(delivered).toBe(true);

    const data = await waitForTerminalStatus(queue, jobId, 15_000);
    expect(data[DOZER_JOB_STATE_KEY]?.r).toMatchObject({
      approved: true,
      payload: { userId: 'user-1' },
    });
  });

  integrationTest('signal workflow: returns null payload on timeout', async () => {
    const engine = moduleRef.get(DozerEngine);
    const id = `signal-timeout-test-${Date.now()}`;

    const jobId = await engine.start('signal-workflow', {
      id,
      timeoutMs: 500,
    });

    const data = await waitForTerminalStatus(queue, jobId, 15_000);
    expect(data[DOZER_JOB_STATE_KEY]?.r).toEqual({
      approved: false,
      payload: null,
    });
  });
});
const toResultQueueJobId = (workflowJobId: string): string => {
  if (/^\d+$/.test(workflowJobId)) {
    return `#${workflowJobId}`;
  }

  return workflowJobId;
};
