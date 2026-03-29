import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Queue } from 'bullmq';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  SerializationError,
  WORKFLOW_STATUS,
  WorkflowJobData,
  WorkflowResultQueueJobData,
} from 'dozer';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import {
  EXAMPLE_RESULT_QUEUE,
  EXAMPLE_WORKFLOW_QUEUE,
} from '../src/infra/tokens';
import { FailureMemoryService } from '../src/support/failure-memory.service';
import { ScenarioControlsService } from '../src/support/scenario-controls.service';
import { StepOutsideFlowWorkflow } from '../src/workflows/step-outside-flow.workflow';
import { ThisStateWorkflow } from '../src/workflows/this-state.workflow';
import { isRedisReachable, redisTestConfig } from './helpers/redis';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

const waitForTerminalStatus = async (
  queue: Queue<WorkflowJobData<unknown>>,
  jobId: string,
  timeoutMs = 20000,
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

  throw new Error(`Timed out waiting for job ${jobId}`);
};

let sequence = 0;
const nextId = (prefix: string): string => {
  sequence += 1;
  return `${prefix}-${Date.now()}-${sequence}`;
};

jest.setTimeout(90000);

describe('Advanced workflows integration', () => {
  let moduleRef: TestingModule;
  let app: INestApplication;
  let queue: Queue<WorkflowJobData<unknown>>;
  let resultQueue: Queue<WorkflowResultQueueJobData<unknown>>;
  let failureMemory: FailureMemoryService;
  let controls: ScenarioControlsService;
  let thisStateWorkflow: ThisStateWorkflow;
  let stepOutsideFlowWorkflow: StepOutsideFlowWorkflow;
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

      process.stderr.write(
        `${message} Advanced integration tests are skipped.\n`,
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
    failureMemory = app.get(FailureMemoryService);
    controls = app.get(ScenarioControlsService);
    thisStateWorkflow = app.get(ThisStateWorkflow);
    stepOutsideFlowWorkflow = app.get(StepOutsideFlowWorkflow);
    engine = app.get(DozerEngine);
  }, 30000);

  beforeEach(() => {
    if (!redisAvailable) {
      return;
    }

    failureMemory.reset();
    controls.reset();
    thisStateWorkflow.current = undefined;
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
    'fails workflow run with invalid input parameters',
    async () => {
      const start = await request(app.getHttpServer())
        .post('/workflows/input-validation/start')
        .send({ orderId: 'bad' })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      const state = failed[DOZER_JOB_STATE_KEY];

      expect(state?.s).toBe(WORKFLOW_STATUS.failed);
      expect(String(state?.e ?? '')).toContain('invalid-input');
      expect(Object.keys(state?.c ?? {})).toHaveLength(0);
    },
  );

  integrationTest(
    'fails job execution for invalid workflow identifier',
    async () => {
      const start = await request(app.getHttpServer())
        .post('/workflows/removed-workflow/start')
        .send({ value: 1 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      const state = failed[DOZER_JOB_STATE_KEY];

      expect(state?.s).toBe(WORKFLOW_STATUS.failed);
      expect(String(state?.e ?? '')).toContain('not registered');
    },
  );

  integrationTest('supports duplicate step names in one workflow', async () => {
    const id = nextId('duplicate-step');

    const start = await request(app.getHttpServer())
      .post('/workflows/duplicate-step-name/start')
      .send({ id, value: 1 })
      .expect(201);

    const failed = await waitForTerminalStatus(queue, start.body.jobId);
    const failedState = failed[DOZER_JOB_STATE_KEY];

    expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
    expect(failedState?.c['0:same-name']).toBe(2);
    expect(failedState?.c['1:same-name']).toBeUndefined();

    await request(app.getHttpServer())
      .post(`/workflows/${start.body.jobId}/replay`)
      .send({})
      .expect(201);

    const completed = await waitForTerminalStatus(queue, start.body.jobId);
    const completedState = completed[DOZER_JOB_STATE_KEY];
    expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
    expect(completedState?.c['0:same-name']).toBe(2);
    expect(completedState?.c['1:same-name']).toBe(4);
    expect(failureMemory.calls(`duplicate-step:first:${id}`)).toBe(1);
    expect(failureMemory.calls(`duplicate-step:second:${id}`)).toBe(2);
  });

  integrationTest(
    'supports workflow inheritance and polymorphic step override',
    async () => {
      const id = nextId('inheritance');

      const start = await request(app.getHttpServer())
        .post('/workflows/inheritance/start')
        .send({ id, value: 2 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      const failedState = failed[DOZER_JOB_STATE_KEY];
      expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
      expect(failedState?.c['0:base-step']).toBe(3);
      expect(failedState?.c['1:poly-step']).toBe(9);

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(queue, start.body.jobId);
      const completedState = completed[DOZER_JOB_STATE_KEY];
      expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
      expect(completedState?.r).toEqual({ value: 9 });
      expect(failureMemory.calls(`inheritance:base:${id}`)).toBe(1);
      expect(failureMemory.calls(`inheritance:poly-override:${id}`)).toBe(1);
      expect(failureMemory.calls(`inheritance:fail:${id}`)).toBe(2);
    },
  );

  integrationTest(
    'supports inherited dispatch when override has @Step with different name',
    async () => {
      const id = nextId('inheritance-dispatch');

      const start = await request(app.getHttpServer())
        .post('/workflows/inheritance-dispatch/start')
        .send({ id, value: 2 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      const failedState = failed[DOZER_JOB_STATE_KEY];
      expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
      expect(failedState?.c['0:dispatch']).toBe(7);
      expect(failedState?.c['0.0:target-derived']).toBeUndefined();
      expect(failedState?.c['0.0:target-base']).toBeUndefined();

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(queue, start.body.jobId);
      const completedState = completed[DOZER_JOB_STATE_KEY];
      expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
      expect(completedState?.r).toEqual({ value: 7 });
      expect(
        failureMemory.calls(`inheritance-dispatch:target-derived:${id}`),
      ).toBe(1);
      expect(
        failureMemory.calls(`inheritance-dispatch:target-base:${id}`),
      ).toBe(0);
      expect(failureMemory.calls(`inheritance-dispatch:fail:${id}`)).toBe(2);
    },
  );

  integrationTest(
    're-executes inherited override without @Step when base method calls it',
    async () => {
      const id = nextId('inheritance-plain');

      const start = await request(app.getHttpServer())
        .post('/workflows/inheritance-plain-override/start')
        .send({ id, value: 2 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      const failedState = failed[DOZER_JOB_STATE_KEY];

      expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
      expect(
        Object.keys(failedState?.c ?? {}).some((key) => key.includes('target')),
      ).toBe(false);

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(queue, start.body.jobId);
      const completedState = completed[DOZER_JOB_STATE_KEY];

      expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
      expect(completedState?.r).toEqual({ value: 9 });
      expect(
        Object.keys(completedState?.c ?? {}).some((key) =>
          key.includes('target'),
        ),
      ).toBe(false);
      expect(
        failureMemory.calls(`inheritance-plain:target-derived:${id}`),
      ).toBe(2);
      expect(failureMemory.calls(`inheritance-plain:target-base:${id}`)).toBe(
        0,
      );
      expect(failureMemory.calls(`inheritance-plain:fail:${id}`)).toBe(2);
    },
  );

  integrationTest('supports workflow without @Step methods', async () => {
    const id = nextId('no-step');

    const start = await request(app.getHttpServer())
      .post('/workflows/no-step/start')
      .send({ id, value: 10 })
      .expect(201);

    const failed = await waitForTerminalStatus(queue, start.body.jobId);
    const failedState = failed[DOZER_JOB_STATE_KEY];

    expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
    expect(Object.keys(failedState?.c ?? {})).toHaveLength(0);

    await request(app.getHttpServer())
      .post(`/workflows/${start.body.jobId}/replay`)
      .send({})
      .expect(201);

    const completed = await waitForTerminalStatus(queue, start.body.jobId);
    const completedState = completed[DOZER_JOB_STATE_KEY];

    expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
    expect(completedState?.r).toEqual({ value: 11 });
    expect(Object.keys(completedState?.c ?? {})).toHaveLength(0);
    expect(failureMemory.calls(`no-step:run:${id}`)).toBe(2);
  });

  integrationTest(
    'supports binary arguments and fails on non-serializable payloads',
    async () => {
      const id = nextId('binary-payload');
      const bytes = new Uint8Array([1, 2, 3, 4]);
      const arrayBuffer = bytes.buffer.slice(0);
      const buffer = Buffer.from([5, 6, 7]);
      const blob =
        typeof Blob === 'undefined'
          ? undefined
          : new Blob([bytes], { type: 'application/octet-stream' });

      const jobId = await engine.start('binary-payload', {
        id,
        bytes,
        arrayBuffer,
        buffer,
        blob,
      });

      const failed = await waitForTerminalStatus(queue, jobId);
      const failedState = failed[DOZER_JOB_STATE_KEY];
      expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
      expect(String(failedState?.e ?? '')).toContain(
        'binary-payload-fail-once',
      );

      await request(app.getHttpServer())
        .post(`/workflows/${jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(queue, jobId);
      const completedState = completed[DOZER_JOB_STATE_KEY];
      expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
      expect(completedState?.r).toEqual({
        sum: 10,
        isUint8Array: true,
        isArrayBuffer: true,
        isBuffer: true,
        blobSize: blob?.size ?? 0,
      });
      expect(failureMemory.calls(`binary-payload:inspect:${id}`)).toBe(1);
      expect(failureMemory.calls(`binary-payload:fail:${id}`)).toBe(2);

      await expect(
        engine.start('binary-payload', {
          id: nextId('binary-invalid'),
          bad: {
            fn: () => 1,
          },
        }),
      ).rejects.toBeInstanceOf(SerializationError);
    },
  );

  integrationTest('supports step calls outside workflow run', async () => {
    const direct = stepOutsideFlowWorkflow.increment(41);
    expect(await Promise.resolve(direct)).toBe(42);

    const start = await request(app.getHttpServer())
      .post('/workflows/step-outside-flow/start')
      .send({})
      .expect(201);

    const completed = await waitForTerminalStatus(queue, start.body.jobId);
    const state = completed[DOZER_JOB_STATE_KEY];

    expect(state?.s).toBe(WORKFLOW_STATUS.completed);
    expect(state?.c['0:increment']).toBe(6);
    expect(
      Object.keys(state?.c ?? {}).some((key) =>
        key.includes('seed-from-constructor'),
      ),
    ).toBe(false);
  });

  integrationTest(
    'detects run-level nondeterminism from timer branches',
    async () => {
      const id = nextId('timer');
      controls.setTimerTick(id, 2);

      const start = await request(app.getHttpServer())
        .post('/workflows/run-source-nondeterministic/start')
        .send({ id, source: 'timer', value: 1 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      expect(failed[DOZER_JOB_STATE_KEY]?.c['0:timer-even']).toBeDefined();

      controls.setTimerTick(id, 3);
      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(500);

      const replayedJob = await queue.getJob(start.body.jobId);
      const replayedState = replayedJob?.data[DOZER_JOB_STATE_KEY];
      expect(replayedState?.s).toBe(WORKFLOW_STATUS.failed);
      expect(String(replayedState?.e ?? '')).toContain('Step replay conflict');
    },
  );

  integrationTest(
    'detects run-level nondeterminism from random and external sources',
    async () => {
      const randomId = nextId('random');
      controls.setRandomValue(randomId, 0.1);

      const randomStart = await request(app.getHttpServer())
        .post('/workflows/run-source-nondeterministic/start')
        .send({ id: randomId, source: 'random', value: 1 })
        .expect(201);

      await waitForTerminalStatus(queue, randomStart.body.jobId);
      controls.setRandomValue(randomId, 0.9);

      await request(app.getHttpServer())
        .post(`/workflows/${randomStart.body.jobId}/replay`)
        .send({})
        .expect(500);

      const randomJob = await queue.getJob(randomStart.body.jobId);
      expect(String(randomJob?.data[DOZER_JOB_STATE_KEY]?.e ?? '')).toContain(
        'Step replay conflict',
      );

      const externalId = nextId('external');
      controls.setExternalValue(externalId, 'a');

      const externalStart = await request(app.getHttpServer())
        .post('/workflows/run-source-nondeterministic/start')
        .send({ id: externalId, source: 'external', value: 1 })
        .expect(201);

      await waitForTerminalStatus(queue, externalStart.body.jobId);
      controls.setExternalValue(externalId, 'b');

      await request(app.getHttpServer())
        .post(`/workflows/${externalStart.body.jobId}/replay`)
        .send({})
        .expect(500);

      const externalJob = await queue.getJob(externalStart.body.jobId);
      expect(String(externalJob?.data[DOZER_JOB_STATE_KEY]?.e ?? '')).toContain(
        'Step replay conflict',
      );
    },
  );

  integrationTest(
    'restores workflow-local this-state via cached step results',
    async () => {
      const id = nextId('this-state');

      const start = await request(app.getHttpServer())
        .post('/workflows/this-state/start')
        .send({ id, value: 9 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      expect(failed[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.failed);
      expect(failed[DOZER_JOB_STATE_KEY]?.c['0:hydrate']).toBeDefined();

      thisStateWorkflow.current = undefined;

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(queue, start.body.jobId);
      expect(completed[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
      expect(completed[DOZER_JOB_STATE_KEY]?.r).toEqual({
        ok: true,
        token: `${id}:9`,
      });
      expect(failureMemory.calls(`this-state:hydrate:${id}`)).toBe(1);
      expect(failureMemory.calls(`this-state:verify:${id}`)).toBe(2);
    },
  );

  integrationTest('supports deep nested recursion', async () => {
    const start = await request(app.getHttpServer())
      .post('/workflows/recursive/start')
      .send({ value: 1, depth: 4 })
      .expect(201);

    const completed = await waitForTerminalStatus(queue, start.body.jobId);
    const compactState = completed[DOZER_JOB_STATE_KEY];
    expect(compactState?.s).toBe(WORKFLOW_STATUS.completed);
    expect(compactState?.c['0:node']).toBeDefined();
    expect(compactState?.c['0.0:node']).toBeUndefined();
    expect(compactState?.c['0.0.0:node']).toBeUndefined();
    expect(compactState?.c['0.0.0.0:node']).toBeUndefined();
    expect(compactState?.c['0.0.0.0.0:node']).toBeUndefined();
    expect(compactState?.c['0.0.0.0.0.0:leaf']).toBeUndefined();
    expect(compactState?.t).toEqual([
      '0:node',
      '0.0:node',
      '0.0.0:node',
      '0.0.0.0:node',
      '0.0.0.0.0:node',
      '0.0.0.0.0.0:leaf',
    ]);
  });

  integrationTest(
    'keeps action-level nondeterministic result stable on replay',
    async () => {
      const id = nextId('action-nondet');

      const start = await request(app.getHttpServer())
        .post('/workflows/action-nondeterministic/start')
        .send({ id })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      const firstRandom = failed[DOZER_JOB_STATE_KEY]?.c['0:randomize'];
      expect(typeof firstRandom).toBe('number');

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(queue, start.body.jobId);
      const secondRandom = completed[DOZER_JOB_STATE_KEY]?.c['0:randomize'];

      expect(secondRandom).toBe(firstRandom);
      expect(failureMemory.calls(`action-nondet:random:${id}`)).toBe(1);
    },
  );

  integrationTest(
    'supports nested workflow invocation and waits for child completion',
    async () => {
      const id = nextId('parent');

      const start = await request(app.getHttpServer())
        .post('/workflows/parent-workflow/start')
        .send({ id, value: 3 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      const invokeChild = failed[DOZER_JOB_STATE_KEY]?.c['0:invoke-child'] as
        | { childJobId: string; childValue: number }
        | undefined;

      expect(failed[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.failed);
      expect(invokeChild?.childJobId).toBeDefined();
      expect(invokeChild?.childValue).toBe(6);

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(queue, start.body.jobId);
      const completedState = completed[DOZER_JOB_STATE_KEY];

      expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
      expect(failureMemory.calls(`child:compute:${id}`)).toBe(1);
      expect(failureMemory.calls(`parent:finalize:${id}`)).toBe(2);
    },
  );

  integrationTest(
    'propagates child workflow failures and recovers parent workflow on replay',
    async () => {
      const id = nextId('parent-child-fail');

      const start = await request(app.getHttpServer())
        .post('/workflows/parent-child-failing/start')
        .send({ id, value: 4 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      const failedState = failed[DOZER_JOB_STATE_KEY];

      expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
      expect(String(failedState?.e ?? '')).toContain(
        'child-failing-process-fail-once',
      );
      expect(failedState?.c['0:invoke-child']).toBeUndefined();

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(queue, start.body.jobId);
      const completedState = completed[DOZER_JOB_STATE_KEY];

      expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
      expect(completedState?.r).toEqual({ value: 11 });
      expect(failureMemory.calls(`child-failing:prepare:${id}`)).toBe(2);
      expect(failureMemory.calls(`child-failing:process:${id}`)).toBe(2);
    },
  );

  integrationTest(
    'supports deep workflow nesting with random step timing and replay',
    async () => {
      const id = nextId('parent-deep');

      const start = await request(app.getHttpServer())
        .post('/workflows/parent-deep-workflow/start')
        .send({ id, value: 1 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      const failedState = failed[DOZER_JOB_STATE_KEY];

      expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
      expect(String(failedState?.e ?? '')).toContain('child-deep-fail-once');

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(queue, start.body.jobId);
      const completedState = completed[DOZER_JOB_STATE_KEY];

      expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
      expect(completedState?.r).toEqual({ value: 4 });
      expect(failureMemory.calls(`grandchild:work:${id}`)).toBe(2);
      expect(failureMemory.calls(`child-deep:fail:${id}`)).toBe(2);
    },
  );

  integrationTest(
    'supports sequential batch execution with shared await',
    async () => {
      const id = nextId('batch-sequential');
      const start = await request(app.getHttpServer())
        .post('/workflows/batch-wait/start')
        .send({ id, values: [1, 2, 3] })
        .expect(201);

      const completed = await waitForTerminalStatus(queue, start.body.jobId);
      const state = completed[DOZER_JOB_STATE_KEY];
      expect(state?.s).toBe(WORKFLOW_STATUS.completed);
      expect(state?.r).toEqual({ sum: 9, count: 3 });
      expect(state?.c['0:unit']).toBeDefined();
      expect(state?.c['1:unit']).toBeDefined();
      expect(state?.c['2:unit']).toBeDefined();
      expect(state?.c['3:aggregate']).toEqual({ sum: 9, count: 3 });
      expect(failureMemory.calls(`batch-wait:unit:${id}`)).toBe(3);
    },
  );

  integrationTest(
    'serializes Date values and restores cached step result on replay',
    async () => {
      const id = nextId('date-serialization');
      const at = new Date('2025-06-01T12:00:00.000Z');

      const jobId = await engine.start('date-serialization', {
        id,
        at,
      });

      const failed = await waitForTerminalStatus(queue, jobId);
      const failedState = failed[DOZER_JOB_STATE_KEY];
      expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
      expect(String(failedState?.e ?? '')).toContain(
        'date-serialization-fail-once',
      );

      const result = (await engine.run(jobId)) as {
        iso: string;
        at: Date;
        plusMs: Date;
        isDate: boolean;
      };

      expect(result.iso).toBe('2025-06-01T12:00:00.000Z');
      expect(result.isDate).toBe(true);
      expect(result.at).toBeInstanceOf(Date);
      expect(result.plusMs).toBeInstanceOf(Date);
      expect(result.at.toISOString()).toBe('2025-06-01T12:00:00.000Z');
      expect(result.plusMs.toISOString()).toBe('2025-06-01T12:00:01.234Z');
      expect(failureMemory.calls(`date-serialization:read:${id}`)).toBe(1);
      expect(failureMemory.calls(`date-serialization:fail:${id}`)).toBe(2);

      const job = await queue.getJob(jobId);
      const state = job?.data[DOZER_JOB_STATE_KEY];
      expect(state?.s).toBe(WORKFLOW_STATUS.completed);
      expect(
        (
          state?.c['0:read-date'] as {
            at?: { __dozer_serialized__?: string };
          }
        )?.at?.__dozer_serialized__,
      ).toBe('date');
    },
  );

  integrationTest('supports mix of sync and async steps', async () => {
    const start = await request(app.getHttpServer())
      .post('/workflows/sync-async/start')
      .send({ value: 3 })
      .expect(201);

    const completed = await waitForTerminalStatus(queue, start.body.jobId);
    const compactState = completed[DOZER_JOB_STATE_KEY];

    expect(compactState?.s).toBe(WORKFLOW_STATUS.completed);
    expect(compactState?.c['0:sync-step']).toBe(4);
    expect(compactState?.c['1:async-step']).toBe(8);
    expect(compactState?.r).toBe(8);
  });

  integrationTest(
    're-executes plain methods that are not decorated with @Step',
    async () => {
      const id = nextId('missing-step');

      const start = await request(app.getHttpServer())
        .post('/workflows/missing-step/start')
        .send({ id, value: 5 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      expect(failed[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.failed);
      expect(
        Object.keys(failed[DOZER_JOB_STATE_KEY]?.c ?? {}).some((key) =>
          key.includes('plain'),
        ),
      ).toBe(false);

      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(201);

      const completed = await waitForTerminalStatus(queue, start.body.jobId);
      expect(completed[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
      expect(failureMemory.calls(`missing-step:plain:${id}`)).toBe(2);
      expect(failureMemory.calls(`missing-step:fail:${id}`)).toBe(2);
    },
  );

  integrationTest(
    'detects version skew when workflow logic changes before replay',
    async () => {
      const id = nextId('versioned');
      controls.setVersion(id, 'v1');

      const start = await request(app.getHttpServer())
        .post('/workflows/versioned-logic/start')
        .send({ id, value: 1 })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      expect(failed[DOZER_JOB_STATE_KEY]?.c['0:logic-v1']).toBeDefined();

      controls.setVersion(id, 'v2');
      await request(app.getHttpServer())
        .post(`/workflows/${start.body.jobId}/replay`)
        .send({})
        .expect(500);

      const replayedJob = await queue.getJob(start.body.jobId);
      const replayedState = replayedJob?.data[DOZER_JOB_STATE_KEY];
      expect(replayedState?.s).toBe(WORKFLOW_STATUS.failed);
      expect(String(replayedState?.e ?? '')).toContain('Step replay conflict');
    },
  );

  integrationTest(
    'calls onFailed method on workflow instance after terminal failure',
    async () => {
      const id = nextId('on-failed');

      const start = await request(app.getHttpServer())
        .post('/workflows/on-failed/start')
        .send({ id })
        .expect(201);

      const failed = await waitForTerminalStatus(queue, start.body.jobId);
      const state = failed[DOZER_JOB_STATE_KEY];

      expect(state?.s).toBe(WORKFLOW_STATUS.failed);
      expect(failureMemory.calls(`on-failed:callback:${id}`)).toBe(1);
    },
  );

  integrationTest(
    'publishes failure payload to result queue when publishOnFailure is true',
    async () => {
      const id = nextId('failure-publish');

      const start = await request(app.getHttpServer())
        .post('/workflows/failure-publish/start')
        .send({ id })
        .expect(201);

      const { jobId } = start.body as { jobId: string };
      const failed = await waitForTerminalStatus(queue, jobId);
      const state = failed[DOZER_JOB_STATE_KEY];

      expect(state?.s).toBe(WORKFLOW_STATUS.failed);

      const resultJobId = `#${jobId}`;
      const resultJob = await resultQueue.getJob(resultJobId);
      expect(resultJob).toBeDefined();
      expect(resultJob?.name).toBe('workflow-result');
      expect(resultJob?.data.status).toBe('failed');
      expect(resultJob?.data.result).toBeNull();
      expect(resultJob?.data.error).toContain(`failure-publish-error:${id}`);
      expect(resultJob?.data.jobId).toBe(jobId);
      expect(resultJob?.data.workflowName).toBe('failure-publish');
    },
  );
});
