import { Test, TestingModule } from '@nestjs/testing';
import { DelayedError } from 'bullmq';
import {
  DOZER_JOB_STATE_KEY,
  DozerClient,
  DozerEngine,
  DozerModule,
  DozerWorkflow,
  InMemoryWorkflowQueue,
  Step,
  Workflow,
  WORKFLOW_STATUS,
} from './index';

@Workflow({ name: 'signal-workflow' })
class SignalWorkflow extends DozerWorkflow<{ value: number }> {
  @Step({ name: 'before' })
  before(v: number): Promise<number> {
    return Promise.resolve(v + 10);
  }

  @Step({ name: 'after' })
  after(v: number, bonus: number): Promise<number> {
    return Promise.resolve(v + bonus);
  }

  async run(input: { value: number }): Promise<number> {
    const base = await this.before(input.value);
    const payload = await this.waitForSignal<{ bonus: number }>('bonus-received');
    return this.after(base, payload?.bonus ?? 0);
  }
}

@Workflow({ name: 'signal-timeout-workflow' })
class SignalTimeoutWorkflow extends DozerWorkflow<unknown> {
  async run(): Promise<{ timedOut: boolean }> {
    const result = await this.waitForSignal<{ value: number }>('event', {
      timeoutMs: 100,
    });
    return { timedOut: result === null };
  }
}

describe('DozerEngine signal integration', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;
  let client: DozerClient;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature([SignalWorkflow, SignalTimeoutWorkflow]),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
    client = moduleRef.get(DozerClient);
  });

  afterEach(async () => {
    if (moduleRef) await moduleRef.close();
  });

  it('engine throws DelayedError when waiting for signal', async () => {
    const jobId = await engine.start('signal-workflow', { value: 5 });
    await expect(engine.run(jobId)).rejects.toBeInstanceOf(DelayedError);
  });

  it('job is delayed after waitForSignal()', async () => {
    const jobId = await engine.start('signal-workflow', { value: 5 });
    try { await engine.run(jobId); } catch {}
    expect(queue.isDelayed(jobId)).toBe(true);
  });

  it('sendSignal returns false when no pending signal', async () => {
    const jobId = await engine.start('signal-workflow', { value: 5 });
    // job not yet run — no pending signal registered
    const result = await client.sendSignal(jobId, 'bonus-received', { bonus: 42 });
    expect(result).toBe(false);
  });

  it('sendSignal returns true and promotes job after workflow starts waiting', async () => {
    const jobId = await engine.start('signal-workflow', { value: 5 });
    try { await engine.run(jobId); } catch {}

    const delivered = await client.sendSignal(jobId, 'bonus-received', { bonus: 42 });
    expect(delivered).toBe(true);
    expect(queue.isDelayed(jobId)).toBe(false); // promoted
  });

  it('workflow completes with signal payload', async () => {
    const jobId = await engine.start('signal-workflow', { value: 5 });
    // First run: parks waiting for signal
    try { await engine.run(jobId); } catch {}

    // Deliver signal
    await client.sendSignal(jobId, 'bonus-received', { bonus: 7 });

    // Second run: replays before-step, finds signal in cache, runs after-step
    const result = await engine.run(jobId);
    expect(result).toBe(22); // (5+10) + 7
  });

  it('waitForSignal returns null when timeout elapses', async () => {
    const jobId = await engine.start('signal-timeout-workflow', {});
    // First run: registers signal with 100ms timeout → parks
    try { await engine.run(jobId); } catch {}

    // Manipulate state to simulate timeout elapsed
    const job = await queue.get(jobId);
    const state = job!.data[DOZER_JOB_STATE_KEY]!;
    const signalEntry = Object.values(state.ps ?? {})[0];
    if (signalEntry) {
      signalEntry.e = Date.now() - 1; // set expiry to past
      await job!.updateData(job!.data);
    }
    await queue.promoteDelayed(jobId);

    // Second run: detects timeout → saves null result → returns { timedOut: true }
    const result = await engine.run(jobId) as { timedOut: boolean };
    expect(result.timedOut).toBe(true);
  });
});
