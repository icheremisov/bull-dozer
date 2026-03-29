import { Test } from '@nestjs/testing';
import {
  DozerClient,
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  Workflow,
} from './index';
import { DozerWorkflow } from './workflow/dozer-workflow';

@Workflow({ name: 'duplicate-workflow-name' })
class DuplicateNameWorkflowA extends DozerWorkflow<unknown> {
  run(): Promise<{ ok: true }> {
    return Promise.resolve({ ok: true });
  }
}

@Workflow({ name: 'duplicate-workflow-name' })
class DuplicateNameWorkflowB extends DozerWorkflow<unknown> {
  run(): Promise<{ ok: true }> {
    return Promise.resolve({ ok: true });
  }
}

describe('DozerModule.forRootAsync()', () => {
  it('initializes engine and client via async factory', async () => {
    const queue = new InMemoryWorkflowQueue();
    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRootAsync({
          useFactory: () => ({ driver: queue }),
        }),
      ],
    }).compile();
    await moduleRef.init();

    try {
      expect(moduleRef.get(DozerEngine)).toBeDefined();
      expect(moduleRef.get(DozerClient)).toBeDefined();
    } finally {
      await moduleRef.close();
    }
  });

  it('injects dependencies into async factory via imports', async () => {
    const queue = new InMemoryWorkflowQueue();
    const QUEUE_TOKEN = 'QUEUE_TOKEN';

    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRootAsync({
          imports: [
            {
              module: class QueueModule {},
              providers: [{ provide: QUEUE_TOKEN, useValue: queue }],
              exports: [QUEUE_TOKEN],
            },
          ],
          inject: [QUEUE_TOKEN],
          useFactory: (q: InMemoryWorkflowQueue) => ({ driver: q }),
        }),
      ],
    }).compile();
    await moduleRef.init();

    try {
      const client = moduleRef.get(DozerClient);
      const jobId = await client.start('test-wf', {});
      const job = await queue.get(jobId);
      expect(job).toBeDefined();
    } finally {
      await moduleRef.close();
    }
  });
});

describe('DozerModule registration constraints', () => {
  it('fails when multiple workflows share the same name', async () => {
    const queue = new InMemoryWorkflowQueue();

    await expect(
      Test.createTestingModule({
        imports: [
          DozerModule.forRoot({
            driver: queue,
          }),
          DozerModule.forFeature([
            DuplicateNameWorkflowA,
            DuplicateNameWorkflowB,
          ]),
        ],
      }).compile(),
    ).rejects.toThrow('already registered');
  });
});
