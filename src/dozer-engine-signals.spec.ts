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
    const payload = await this.waitForSignal<{ bonus: number }>(
      'bonus-received',
    );
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

@Workflow({ name: 'break-then-signal-workflow' })
class BreakThenSignalWorkflow extends DozerWorkflow<{
  value: number;
  wakeUpAt: number;
}> {
  @Step({ name: 'compute' })
  compute(v: number): Promise<number> {
    return Promise.resolve(v + 1);
  }

  async run(input: {
    value: number;
    wakeUpAt: number;
  }): Promise<{ result: number }> {
    this.breakUntil(input.wakeUpAt);
    const payload = await this.waitForSignal<{ bonus: number }>('ready');
    const r = await this.compute(input.value + (payload?.bonus ?? 0));
    return { result: r };
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
        DozerModule.forFeature([
          SignalWorkflow,
          SignalTimeoutWorkflow,
          BreakThenSignalWorkflow,
        ]),
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
    try {
      await engine.run(jobId);
    } catch {
      // intentionally empty
    }
    expect(queue.isDelayed(jobId)).toBe(true);
  });

  it('sendSignal returns false when no pending signal', async () => {
    const jobId = await engine.start('signal-workflow', { value: 5 });
    // job not yet run — no pending signal registered
    const result = await client.sendSignal(jobId, 'bonus-received', {
      bonus: 42,
    });
    expect(result).toBe(false);
  });

  it('sendSignal returns true and promotes job after workflow starts waiting', async () => {
    const jobId = await engine.start('signal-workflow', { value: 5 });
    try {
      await engine.run(jobId);
    } catch {
      // intentionally empty
    }

    const delivered = await client.sendSignal(jobId, 'bonus-received', {
      bonus: 42,
    });
    expect(delivered).toBe(true);
    expect(queue.isDelayed(jobId)).toBe(false); // promoted
  });

  it('workflow completes with signal payload', async () => {
    const jobId = await engine.start('signal-workflow', { value: 5 });
    // First run: parks waiting for signal
    try {
      await engine.run(jobId);
    } catch {
      // intentionally empty
    }

    // Deliver signal
    await client.sendSignal(jobId, 'bonus-received', { bonus: 7 });

    // Second run: replays before-step, finds signal in cache, runs after-step
    const result = await engine.run(jobId);
    expect(result).toBe(22); // (5+10) + 7
  });

  it('waitForSignal returns null when timeout elapses', async () => {
    const jobId = await engine.start('signal-timeout-workflow', {});
    // First run: registers signal with 100ms timeout → parks
    try {
      await engine.run(jobId);
    } catch {
      // intentionally empty
    }

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
    const result = (await engine.run(jobId)) as { timedOut: boolean };
    expect(result.timedOut).toBe(true);
  });

  it('workflow completes after breakUntil stage followed by signal stage', async () => {
    const wakeUpAt = Date.now() + 100; // 100 ms
    const jobId = await engine.start('break-then-signal-workflow', {
      value: 5,
      wakeUpAt,
    });

    // Run 1: breakUntil timestamp is future → parks as delayed
    try {
      await engine.run(jobId);
    } catch {
      // intentionally empty
    }
    expect(queue.isDelayed(jobId)).toBe(true);

    // Signal before break is done → no pending signal yet → returns false
    const earlySignal = await client.sendSignal(jobId, 'ready', { bonus: 10 });
    expect(earlySignal).toBe(false);

    // Wait for delay to elapse, then promote
    await new Promise<void>((resolve) => setTimeout(resolve, 150));
    await queue.promoteDelayed(jobId);

    // Run 2: breakUntil sees past timestamp → returns. hits waitForSignal → parks
    try {
      await engine.run(jobId);
    } catch {
      // intentionally empty
    }
    expect(queue.isDelayed(jobId)).toBe(true);

    // Now signal is registered — deliver it
    const delivered = await client.sendSignal(jobId, 'ready', { bonus: 10 });
    expect(delivered).toBe(true);
    expect(queue.isDelayed(jobId)).toBe(false);

    // Run 3: breakUntil (past, no-op) → signal cached → compute
    const result = await engine.run(jobId);
    expect(result).toEqual({ result: 16 }); // (5 + 10) + 1
  }, 5_000);

  it('sendSignal with wrong signal name returns false and does not promote job', async () => {
    const jobId = await engine.start('signal-workflow', { value: 5 });
    try {
      await engine.run(jobId);
    } catch {
      // intentionally empty
    }
    expect(queue.isDelayed(jobId)).toBe(true);

    const wrongSignal = await client.sendSignal(jobId, 'other-event', {
      data: 1,
    });
    expect(wrongSignal).toBe(false);
    expect(queue.isDelayed(jobId)).toBe(true);
  });

  it('uses moduleOptions.defaults.signalTimeoutMs as deadline when waitForSignal has no timeoutMs', async () => {
    const customTimeoutMs = 30_000;
    const localQueue = new InMemoryWorkflowQueue();

    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: { signalTimeoutMs: customTimeoutMs },
        }),
        DozerModule.forFeature([SignalWorkflow]),
      ],
    }).compile();
    await localModule.init();

    const localEngine = localModule.get(DozerEngine);
    const jobId = await localEngine.start('signal-workflow', { value: 1 });

    const before = Date.now();
    try {
      await localEngine.run(jobId);
    } catch {
      // intentionally empty
    }

    // Job should be delayed; the deadline stored in BullMQ should be ~now + customTimeoutMs
    // We verify by inspecting the state — ps entry has no expiresAt (that's per-signal)
    // but moveToDelayed was called with our custom deadline
    // We can verify indirectly: job is delayed, and the ps entry has no e field
    const job = await localQueue.get(jobId);
    const ps = job!.data[DOZER_JOB_STATE_KEY]?.ps;
    expect(ps).toBeDefined();
    const signalEntry = Object.values(ps!)[0];
    // waitForSignal in SignalWorkflow has no timeoutMs → expiresAt undefined
    expect(signalEntry.e).toBeUndefined();
    expect(localQueue.isDelayed(jobId)).toBe(true);

    // The engine must have called moveToDelayed with ~now + customTimeoutMs
    // We can check by looking at the delayedAt stored in InMemoryWorkflowQueue
    const delayedAt = localQueue.getDelayedAt(jobId);
    expect(delayedAt).toBeGreaterThanOrEqual(before + customTimeoutMs - 100);
    expect(delayedAt).toBeLessThanOrEqual(before + customTimeoutMs + 1000);

    await localModule.close();
  });
});
