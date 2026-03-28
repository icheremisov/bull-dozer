import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import { DelayedError } from 'bullmq';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  DozerModule,
  DozerWorkflow,
  InMemoryWorkflowQueue,
  Step,
  Workflow,
} from './index';

@Injectable()
class SleepStats {
  runs = 0;
  checkCalls = 0;
}

@Workflow({ name: 'polling-workflow' })
class PollingWorkflow extends DozerWorkflow<{ maxChecks: number }> {
  constructor(private readonly stats: SleepStats) {
    super();
  }

  @Step({ name: 'check' })
  async checkStatus(): Promise<boolean> {
    this.stats.checkCalls += 1;
    return this.stats.checkCalls >= 3;
  }

  async run(input: { maxChecks: number }): Promise<{ checks: number }> {
    this.stats.runs += 1;
    let done = false;
    while (!done) {
      await this.sleep(10_000);
      done = await this.checkStatus();
    }
    return { checks: this.stats.checkCalls };
  }
}

@Workflow({ name: 'sleep-once-workflow' })
class SleepOnceWorkflow extends DozerWorkflow<{ value: number }> {
  @Step({ name: 'before' })
  before(v: number): Promise<number> {
    return Promise.resolve(v + 1);
  }

  @Step({ name: 'after' })
  after(v: number): Promise<number> {
    return Promise.resolve(v * 2);
  }

  async run(input: { value: number }): Promise<number> {
    const a = await this.before(input.value);
    await this.sleep(5_000);
    return this.after(a);
  }
}

describe('DozerEngine sleep integration', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;
  let stats: SleepStats;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature(
          [PollingWorkflow, SleepOnceWorkflow],
          [SleepStats],
        ),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
    stats = moduleRef.get(SleepStats);
  });

  afterEach(async () => {
    if (moduleRef) await moduleRef.close();
  });

  it('engine throws DelayedError when workflow calls sleep()', async () => {
    const jobId = await engine.start('sleep-once-workflow', { value: 5 });
    await expect(engine.run(jobId)).rejects.toBeInstanceOf(DelayedError);
  });

  it('job is marked as delayed in the queue after sleep', async () => {
    const jobId = await engine.start('sleep-once-workflow', { value: 5 });
    try { await engine.run(jobId); } catch {}
    expect(queue.isDelayed(jobId)).toBe(true);
  });

  it('workflow completes correctly after being promoted from sleep', async () => {
    const jobId = await engine.start('sleep-once-workflow', { value: 5 });
    // First run: stops at sleep
    try { await engine.run(jobId); } catch {}
    expect(queue.isDelayed(jobId)).toBe(true);

    // Simulate BullMQ waking the job (set wakeUpAt to past)
    const job = await queue.get(jobId);
    const state = job!.data[DOZER_JOB_STATE_KEY]!;
    const sleepKey = Object.keys(state.sl ?? {})[0];
    state.sl![sleepKey] = Date.now() - 1; // mark as already elapsed
    await job!.updateData(job!.data);

    await queue.promoteDelayed(jobId);

    // Second run: replays before-step from cache, completes sleep, runs after-step
    const result = await engine.run(jobId);
    expect(result).toBe(12); // (5+1) * 2
  });

  it('polling workflow runs checkStatus the correct number of times across resumes', async () => {
    const jobId = await engine.start('polling-workflow', { maxChecks: 3 });

    // First resume: sleep → DelayedError. checkStatus not called yet (sleep happens first)
    try { await engine.run(jobId); } catch {}
    expect(stats.checkCalls).toBe(0);

    const advanceSleep = async (): Promise<void> => {
      const job = await queue.get(jobId);
      const state = job!.data[DOZER_JOB_STATE_KEY]!;
      if (state.sl) {
        for (const key of Object.keys(state.sl)) {
          state.sl[key] = Date.now() - 1;
        }
        await job!.updateData(job!.data);
      }
      await queue.promoteDelayed(jobId);
    };

    // Second resume: sleep completes, checkStatus called (returns false → sleep again)
    await advanceSleep();
    try { await engine.run(jobId); } catch {}
    expect(stats.checkCalls).toBe(1);

    // Third resume: sleep completes, checkStatus called (returns false → sleep again)
    await advanceSleep();
    try { await engine.run(jobId); } catch {}
    expect(stats.checkCalls).toBe(2);

    // Fourth resume: sleep completes, checkStatus called (returns true → workflow ends)
    await advanceSleep();
    const result = await engine.run(jobId) as { checks: number };
    expect(result.checks).toBe(3);
    expect(stats.runs).toBe(4);
  });
});
