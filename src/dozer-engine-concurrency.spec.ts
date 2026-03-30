import { Test, TestingModule } from '@nestjs/testing';
import { DelayedError } from 'bullmq';
import {
  DozerEngine,
  DozerModule,
  DozerWorkflow,
  InMemoryWorkflowQueue,
  Step,
  Workflow,
} from './index';

// ---------------------------------------------------------------------------
// Workflow fixture
// ---------------------------------------------------------------------------

@Workflow({ name: 'concurrent-workflow' })
class ConcurrentWorkflow extends DozerWorkflow<{
  id: string;
  wakeUpAt?: number;
}> {
  @Step({ name: 'before' })
  async before(id: string): Promise<string> {
    await this.log(`before:${id}`);
    await this.updateProgress({ phase: 'before', id });
    await this.changePriority({ priority: 1 });
    return id;
  }

  @Step({ name: 'after' })
  async after(id: string): Promise<string> {
    await this.log(`after:${id}`);
    await this.updateProgress({ phase: 'after', id });
    return id;
  }

  async run(input: { id: string; wakeUpAt?: number }): Promise<string> {
    const id = await this.before(input.id);
    if (input.wakeUpAt !== undefined) {
      this.breakUntil(input.wakeUpAt);
    }
    return this.after(id);
  }
}

// ---------------------------------------------------------------------------
// Module setup
// ---------------------------------------------------------------------------

describe('DozerEngine concurrency — _job context isolation', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature([ConcurrentWorkflow]),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
  });

  afterEach(async () => {
    await moduleRef?.close();
  });

  // -------------------------------------------------------------------------
  // Test 1: 10 concurrent jobs — no cross-contamination of _job context
  // -------------------------------------------------------------------------

  it('isolates _job context for each concurrent execution', async () => {
    const ids = Array.from({ length: 10 }, (_, i) => `job-${i}`);

    // Start all 10 jobs
    const jobIds = await Promise.all(
      ids.map((id) => engine.start('concurrent-workflow', { id })),
    );

    // Run all concurrently
    await Promise.all(jobIds.map((jobId) => engine.run(jobId)));

    // Assert each job's logs and progress contain only its own id
    for (let i = 0; i < jobIds.length; i++) {
      const jobId = jobIds[i];
      const id = ids[i];

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toEqual([`before:${id}`, `after:${id}`]);

      const job = await queue.get(jobId);
      expect(job?.progress).toEqual({ phase: 'after', id });
      expect(job?.priority).toBe(1);
    }
  });

  // -------------------------------------------------------------------------
  // Test 2: Re-instantiation after breakUntil — context restored correctly
  //
  // On first engine.run(), the workflow logs 'before:X' then hits breakUntil
  // and throws DelayedError. A brand-new workflow instance is created on the
  // second engine.run(). That second instance must receive the same job via
  // _setJobContext so its 'after:X' log lands on the right job.
  // -------------------------------------------------------------------------

  it('re-establishes _job context on a new instance after breakUntil', async () => {
    const wakeUpAt = Date.now() + 10; // 10 ms in the future

    const jobId = await engine.start('concurrent-workflow', {
      id: 'resume-test',
      wakeUpAt,
    });

    // First run: hits breakUntil, throws DelayedError
    await expect(engine.run(jobId)).rejects.toBeInstanceOf(DelayedError);

    // 'before' step executed — log written on first instance
    const { logs: logsAfterFirstRun } = await queue.getJobLogs(jobId);
    expect(logsAfterFirstRun).toEqual(['before:resume-test']);

    // Wait for the wake-up timestamp to pass
    await new Promise((resolve) => setTimeout(resolve, 20));

    await queue.promoteDelayed(jobId);

    // Second run: new instance, breakUntil passes, 'after' step executes
    await engine.run(jobId);

    // Both logs present — written by two different instances to the same job
    const { logs: logsAfterSecondRun } = await queue.getJobLogs(jobId);
    expect(logsAfterSecondRun).toEqual([
      'before:resume-test',
      'after:resume-test',
    ]);

    const job = await queue.get(jobId);
    expect(job?.progress).toEqual({ phase: 'after', id: 'resume-test' });
  });
});
