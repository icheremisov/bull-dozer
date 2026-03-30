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

// ---------------------------------------------------------------------------
// Workflow fixtures
// ---------------------------------------------------------------------------

@Workflow({ name: 'break-until-workflow' })
class BreakUntilWorkflow extends DozerWorkflow<{
  value: number;
  wakeUpAt: number;
}> {
  @Step({ name: 'before' })
  before(v: number): Promise<number> {
    return Promise.resolve(v + 1);
  }

  @Step({ name: 'after' })
  after(v: number): Promise<number> {
    return Promise.resolve(v * 2);
  }

  async run(input: { value: number; wakeUpAt: number }): Promise<number> {
    const a = await this.before(input.value);
    this.breakUntil(input.wakeUpAt);
    return this.after(a);
  }
}

@Workflow({ name: 'break-for-workflow' })
class BreakForWorkflow extends DozerWorkflow<{ value: number }> {
  @Step({ name: 'step' })
  step(v: number): Promise<number> {
    return Promise.resolve(v + 1);
  }

  async run(input: { value: number }): Promise<number> {
    const a = await this.step(input.value);
    this.breakFor(10_000);
    return a;
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

describe('DozerEngine breakUntil / breakFor integration', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature([BreakUntilWorkflow, BreakForWorkflow]),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
  });

  afterEach(async () => {
    if (moduleRef) await moduleRef.close();
  });

  // -------------------------------------------------------------------------
  // breakUntil
  // -------------------------------------------------------------------------

  describe('breakUntil()', () => {
    it('engine throws DelayedError when timestamp is in the future', async () => {
      const jobId = await engine.start('break-until-workflow', {
        value: 5,
        wakeUpAt: Date.now() + 60_000,
      });
      await expect(engine.run(jobId)).rejects.toBeInstanceOf(DelayedError);
    });

    it('job is marked as delayed in the queue', async () => {
      const jobId = await engine.start('break-until-workflow', {
        value: 5,
        wakeUpAt: Date.now() + 60_000,
      });
      try {
        await engine.run(jobId);
      } catch {
        /* intentionally empty */
      }
      expect(queue.isDelayed(jobId)).toBe(true);
    });

    it('state.sl is NOT written — no sleep intent stored', async () => {
      const jobId = await engine.start('break-until-workflow', {
        value: 5,
        wakeUpAt: Date.now() + 60_000,
      });
      try {
        await engine.run(jobId);
      } catch {
        /* intentionally empty */
      }
      const job = await queue.get(jobId);
      expect(job!.data[DOZER_JOB_STATE_KEY]?.sl).toBeUndefined();
    });

    it('workflow completes immediately when timestamp is already past', async () => {
      const jobId = await engine.start('break-until-workflow', {
        value: 5,
        wakeUpAt: Date.now() - 1,
      });
      const result = await engine.run(jobId);
      expect(result).toBe(12); // (5 + 1) * 2
    });

    it('workflow completes after delay elapses (real 100 ms wait)', async () => {
      const wakeUpAt = Date.now() + 100;
      const jobId = await engine.start('break-until-workflow', {
        value: 5,
        wakeUpAt,
      });

      try {
        await engine.run(jobId);
      } catch {
        /* intentionally empty */
      }
      expect(queue.isDelayed(jobId)).toBe(true);

      await new Promise<void>((resolve) => setTimeout(resolve, 150));
      await queue.promoteDelayed(jobId);

      const result = await engine.run(jobId);
      expect(result).toBe(12); // (5 + 1) * 2
    }, 5_000);
  });

  // -------------------------------------------------------------------------
  // breakFor
  // -------------------------------------------------------------------------

  describe('breakFor()', () => {
    it('engine throws DelayedError when duration > 0', async () => {
      const jobId = await engine.start('break-for-workflow', { value: 5 });
      await expect(engine.run(jobId)).rejects.toBeInstanceOf(DelayedError);
    });

    it('job is marked as delayed in the queue', async () => {
      const jobId = await engine.start('break-for-workflow', { value: 5 });
      try {
        await engine.run(jobId);
      } catch {
        /* intentionally empty */
      }
      expect(queue.isDelayed(jobId)).toBe(true);
    });

    it('state.sl is NOT written — no sleep intent stored', async () => {
      const jobId = await engine.start('break-for-workflow', { value: 5 });
      try {
        await engine.run(jobId);
      } catch {
        /* intentionally empty */
      }
      const job = await queue.get(jobId);
      expect(job!.data[DOZER_JOB_STATE_KEY]?.sl).toBeUndefined();
    });

    it('wakeUpAt is approximately Date.now() + durationMs', async () => {
      const jobId = await engine.start('break-for-workflow', { value: 5 });
      const before = Date.now();
      let caughtWakeUpAt = 0;

      // Intercept moveToDelayed to capture wakeUpAt
      const originalMove: (jid: string, ts: number) => Promise<void> =
        queue.moveToDelayed.bind(queue) as (
          jid: string,
          ts: number,
        ) => Promise<void>;
      queue.moveToDelayed = (jid: string, wakeUpAt: number): Promise<void> => {
        if (jid === jobId) caughtWakeUpAt = wakeUpAt;
        return originalMove(jid, wakeUpAt);
      };

      try {
        await engine.run(jobId);
      } catch {
        /* intentionally empty */
      }

      expect(caughtWakeUpAt).toBeGreaterThanOrEqual(before + 9_900);
      expect(caughtWakeUpAt).toBeLessThanOrEqual(before + 11_000);
    });
  });
});
