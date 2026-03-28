import { Test } from '@nestjs/testing';
import { DozerModule, InMemoryWorkflowQueue, Workflow } from './index';

@Workflow({ name: 'duplicate-workflow-name' })
class DuplicateNameWorkflowA {
  run(): Promise<{ ok: true }> {
    return Promise.resolve({ ok: true });
  }
}

@Workflow({ name: 'duplicate-workflow-name' })
class DuplicateNameWorkflowB {
  run(): Promise<{ ok: true }> {
    return Promise.resolve({ ok: true });
  }
}

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
