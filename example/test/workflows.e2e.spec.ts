import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Queue } from 'bullmq';
import { WORKFLOW_STATUS, WorkflowJobData } from 'dozer';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { setupBullBoard } from '../src/infra/bull-board';
import { EXAMPLE_WORKFLOW_QUEUE } from '../src/infra/tokens';
import { BranchSelectorService } from '../src/support/branch-selector.service';
import { FailureMemoryService } from '../src/support/failure-memory.service';
import { isRedisReachable, redisTestConfig } from './helpers/redis';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

const waitForTerminalStatusByHttp = async (
  app: INestApplication,
  jobId: string,
  timeoutMs = 15000,
): Promise<Record<string, unknown>> => {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const response = await request(app.getHttpServer())
      .get(`/workflows/${jobId}`)
      .expect(200);

    const compactState = response.body.compactState;
    if (
      compactState &&
      (compactState.s === WORKFLOW_STATUS.completed ||
        compactState.s === WORKFLOW_STATUS.failed)
    ) {
      return response.body;
    }

    await sleep(100);
  }

  throw new Error(
    `Timed out waiting for HTTP terminal status for job ${jobId}`,
  );
};

jest.setTimeout(60000);

describe('Example workflows e2e (HTTP + Bull Board)', () => {
  let moduleRef: TestingModule;
  let app: INestApplication;
  let queue: Queue<WorkflowJobData<unknown>>;
  let branchSelector: BranchSelectorService;
  let failureMemory: FailureMemoryService;
  let redisAvailable = false;

  const e2eTest = (name: string, fn: () => Promise<void>): void => {
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

      process.stderr.write(`${message} E2E tests are skipped.\n`);
      return;
    }

    moduleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleRef.createNestApplication();
    app.useLogger(false);
    queue = app.get<Queue<WorkflowJobData<unknown>>>(EXAMPLE_WORKFLOW_QUEUE);
    setupBullBoard(app, queue);
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
  }, 20000);

  e2eTest(
    'starts workflow via HTTP and returns compact state by status API',
    async () => {
      const start = await request(app.getHttpServer())
        .post('/workflows/simple/start')
        .send({ orderId: 321 })
        .expect(201);

      const status = await waitForTerminalStatusByHttp(app, start.body.jobId);
      const compactState = status.compactState as {
        s: number;
        c: Record<string, unknown>;
      };

      expect(status.found).toBe(true);
      expect(compactState.s).toBe(WORKFLOW_STATUS.completed);
      expect(compactState.c['0:validate']).toBeDefined();
      expect(compactState.c['1:process']).toBeDefined();
      expect(compactState.c['2:store']).toBeDefined();
    },
  );

  e2eTest('returns not found status for unknown workflow job id', async () => {
    const response = await request(app.getHttpServer())
      .get('/workflows/not-existing-job')
      .expect(200);

    expect(response.body).toEqual({ found: false });
  });

  e2eTest('serves Bull Board endpoint', async () => {
    const response = await request(app.getHttpServer())
      .get('/admin/queues/')
      .expect(200);

    expect(String(response.text)).toContain('Bull Dashboard');
  });
});
