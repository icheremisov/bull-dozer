import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { Queue } from 'bullmq';
import {
  toWorkflowResultQueueJobId,
  WorkflowJobData,
  WorkflowResultQueueJobData,
} from 'dozer';
import request from 'supertest';
import { AppModule } from '../src/app.module';
import { ClientAppModule } from '../src/client-app.module';
import {
  ClientBatchService,
  type BatchSnapshot,
} from '../src/client/client-batch.service';
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

const waitForBatchByHttp = async (
  app: INestApplication,
  batchId: string,
  timeoutMs = 15000,
): Promise<BatchSnapshot> => {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const response = await request(app.getHttpServer())
      .get(`/client/batches/${batchId}`)
      .expect(200);
    if (response.body?.found && response.body.batch?.pending === 0) {
      return response.body.batch as BatchSnapshot;
    }

    await sleep(100);
  }

  throw new Error(
    `Timed out waiting for client batch "${batchId}" completion.`,
  );
};

jest.setTimeout(60000);

describe('Example client integration (batch producer + result handler)', () => {
  let workerModuleRef: TestingModule;
  let workerApp: INestApplication;
  let clientModuleRef: TestingModule;
  let clientApp: INestApplication;
  let workflowQueue: Queue<WorkflowJobData<unknown>>;
  let resultQueue: Queue<WorkflowResultQueueJobData<unknown>>;
  let batches: ClientBatchService;
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

      process.stderr.write(
        `${message} Client integration tests are skipped.\n`,
      );
      return;
    }

    workerModuleRef = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();
    workerApp = workerModuleRef.createNestApplication();
    workerApp.useLogger(false);
    await workerApp.init();

    clientModuleRef = await Test.createTestingModule({
      imports: [ClientAppModule],
    }).compile();
    clientApp = clientModuleRef.createNestApplication();
    clientApp.useLogger(false);
    await clientApp.init();

    workflowQueue = workerApp.get<Queue<WorkflowJobData<unknown>>>(
      EXAMPLE_WORKFLOW_QUEUE,
    );
    resultQueue =
      workerApp.get<Queue<WorkflowResultQueueJobData<unknown>>>(
        EXAMPLE_RESULT_QUEUE,
      );
    batches = clientApp.get(ClientBatchService);
    branchSelector = workerApp.get(BranchSelectorService);
    failureMemory = workerApp.get(FailureMemoryService);
  }, 30000);

  beforeEach(() => {
    if (!redisAvailable) {
      return;
    }

    branchSelector.reset();
    failureMemory.reset();
    batches.reset();
  });

  afterAll(async () => {
    if (!redisAvailable) {
      return;
    }

    await clientApp.close();
    await workerApp.close();
    await workflowQueue.close().catch(() => undefined);
    await resultQueue.close().catch(() => undefined);
  }, 30000);

  integrationTest(
    'client project submits batch jobs and collects workflow results via result worker',
    async () => {
      const start = await request(clientApp.getHttpServer())
        .post('/client/batches/start')
        .send({
          workflowName: 'result-queue',
          inputs: [{ value: 1 }, { value: 5 }, { value: 9 }],
        })
        .expect(201);

      expect(Array.isArray(start.body.jobIds)).toBe(true);
      expect(start.body.jobIds).toHaveLength(3);

      const batch = await waitForBatchByHttp(clientApp, start.body.batchId);
      expect(batch.workflowName).toBe('result-queue');
      expect(batch.total).toBe(3);
      expect(batch.pending).toBe(0);

      const values = batch.results.map(
        (item) => (item.result as { value: number }).value,
      );
      expect(values).toEqual([2, 6, 10]);
    },
  );

  integrationTest(
    'preserves typed and binary workflow results through result queue and client result handler',
    async () => {
      const { batchId, jobIds } = await batches.submitBatch(
        'result-queue-typed',
        [{ id: 'typed-result-1', seed: 10 }],
      );

      const batch = await batches.waitForBatch(batchId, {
        timeoutMs: 15000,
        pollMs: 100,
      });
      expect(batch.pending).toBe(0);
      expect(batch.completed).toBe(1);

      const workflowJobId = jobIds[0];
      const resultJob = await resultQueue.getJob(
        toWorkflowResultQueueJobId(workflowJobId),
      );
      expect(resultJob).toBeDefined();
      expect(resultJob?.data).toMatchObject({
        jobId: workflowJobId,
        workflowName: 'result-queue-typed',
      });

      const raw = batches.getBatchResultRaw(batchId, workflowJobId) as {
        id: string;
        seed: number;
        at: Date;
        bytes: Uint8Array;
        arrayBuffer: ArrayBuffer;
        buffer: Buffer;
        view: DataView;
        blob?: Blob;
        nested: {
          optional?: string;
          list: [Date, Uint8Array];
        };
      };

      expect(raw.id).toBe('typed-result-1');
      expect(raw.seed).toBe(10);
      expect(raw.at).toBeInstanceOf(Date);
      expect(raw.at.toISOString()).toBe('2026-02-25T12:34:56.000Z');

      expect(raw.bytes).toBeInstanceOf(Uint8Array);
      expect(Array.from(raw.bytes)).toEqual([10, 11, 12]);

      expect(raw.arrayBuffer).toBeInstanceOf(ArrayBuffer);
      expect(Array.from(new Uint8Array(raw.arrayBuffer))).toEqual([10, 11, 12]);

      expect(Buffer.isBuffer(raw.buffer)).toBe(true);
      expect(Array.from(raw.buffer)).toEqual([13, 14]);

      expect(raw.view).toBeInstanceOf(DataView);
      expect(raw.view.getUint8(0)).toBe(10);
      expect(raw.view.getUint8(1)).toBe(11);
      expect(raw.view.getUint8(2)).toBe(12);

      if (typeof Blob !== 'undefined') {
        expect(raw.blob).toBeInstanceOf(Blob);
        expect(raw.blob?.size).toBe(3);
      }

      expect(raw.nested.optional).toBeUndefined();
      expect(raw.nested.list[0]).toBeInstanceOf(Date);
      expect(raw.nested.list[0].toISOString()).toBe('2026-02-25T00:00:00.000Z');
      expect(raw.nested.list[1]).toBeInstanceOf(Uint8Array);
      expect(Array.from(raw.nested.list[1])).toEqual([9, 8, 7]);
    },
  );
});
