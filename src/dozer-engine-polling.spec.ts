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
  WORKFLOW_STATUS,
} from './index';

// ---------------------------------------------------------------------------
// Mock service — controls what getPredictionStatus returns on each call
// ---------------------------------------------------------------------------

type PredictionStatus =
  | { status: 'pending' }
  | { status: 'succeeded'; result: number }
  | { status: 'failed'; error: string };

@Injectable()
class PredictionServiceMock {
  private readonly responses: PredictionStatus[] = [];
  callCount = 0;

  queueResponse(...items: PredictionStatus[]): void {
    this.responses.push(...items);
  }

  getStatus(): PredictionStatus {
    this.callCount += 1;
    const next = this.responses.shift();
    if (!next) {
      throw new Error(
        `PredictionServiceMock has no queued response (call #${this.callCount})`,
      );
    }
    return next;
  }
}

// ---------------------------------------------------------------------------
// Workflow under test — mirrors the real checkAndProcessVideo pattern
// ---------------------------------------------------------------------------

@Workflow({ name: 'polling-step-workflow' })
class PollingStepWorkflow extends DozerWorkflow<{ id: string }> {
  constructor(private readonly svc: PredictionServiceMock) {
    super();
  }

  /**
   * This step body polls an external API in a while-loop, breaking between
   * polls with breakFor(). The step only records ONE entry in the trace,
   * regardless of how many polling iterations are needed.
   */
  @Step({ name: 'poll-prediction' })
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  pollPrediction(_id: string): Promise<{ result: number }> {
    while (true) {
      const status = this.svc.getStatus();
      if (status.status === 'failed') throw new Error(status.error);
      if (status.status === 'succeeded')
        return Promise.resolve({ result: status.result });
      this.breakFor(100); // interrupts; does NOT write a trace entry
    }
  }

  async run(input: { id: string }): Promise<{ result: number }> {
    return this.pollPrediction(input.id);
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('DozerEngine polling step (breakFor inside @Step)', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;
  let svc: PredictionServiceMock;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature([PollingStepWorkflow], [PredictionServiceMock]),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
    svc = moduleRef.get(PredictionServiceMock);
  });

  afterEach(async () => {
    if (moduleRef) await moduleRef.close();
  });

  it('parks as delayed when prediction is still pending', async () => {
    svc.queueResponse({ status: 'pending' });
    const jobId = await engine.start('polling-step-workflow', { id: 'abc' });
    await expect(engine.run(jobId)).rejects.toBeInstanceOf(DelayedError);
    expect(queue.isDelayed(jobId)).toBe(true);
  });

  it('state.sl is never written — breakFor leaves no sleep intent in state', async () => {
    svc.queueResponse({ status: 'pending' });
    const jobId = await engine.start('polling-step-workflow', { id: 'abc' });
    try {
      await engine.run(jobId);
    } catch {
      /* intentionally empty */
    }
    const state = (await queue.get(jobId))!.data[DOZER_JOB_STATE_KEY]!;
    expect(state.sl).toBeUndefined();
  });

  it('trace contains exactly ONE entry for the polling step regardless of iterations', async () => {
    // pending × 2, then succeeded — step body runs 3 times total
    svc.queueResponse(
      { status: 'pending' },
      { status: 'pending' },
      { status: 'succeeded', result: 42 },
    );
    const jobId = await engine.start('polling-step-workflow', { id: 'abc' });

    const runUntilDone = async (): Promise<unknown> => {
      for (;;) {
        try {
          return await engine.run(jobId);
        } catch (err) {
          if (!(err instanceof DelayedError)) throw err;
          await new Promise<void>((r) => setTimeout(r, 150));
          await queue.promoteDelayed(jobId);
        }
      }
    };

    const result = await runUntilDone();
    expect(result).toEqual({ result: 42 });

    const state = (await queue.get(jobId))!.data[DOZER_JOB_STATE_KEY]!;
    // Only ONE trace entry — no trace growth from breakFor
    expect(state.t).toHaveLength(1);
    expect(state.t[0]).toBe('0:poll-prediction');
  }, 10_000);

  it('workflow completes on the first run when prediction is already succeeded', async () => {
    svc.queueResponse({ status: 'succeeded', result: 7 });
    const jobId = await engine.start('polling-step-workflow', { id: 'abc' });
    const result = await engine.run(jobId);
    expect(result).toEqual({ result: 7 });
    expect(svc.callCount).toBe(1);
  });

  it('step body re-executes on each resume until prediction succeeds', async () => {
    svc.queueResponse(
      { status: 'pending' },
      { status: 'pending' },
      { status: 'succeeded', result: 99 },
    );
    const jobId = await engine.start('polling-step-workflow', { id: 'abc' });

    // Run 1: pending → delayed
    try {
      await engine.run(jobId);
    } catch {
      /* intentionally empty */
    }
    expect(svc.callCount).toBe(1);
    expect(queue.isDelayed(jobId)).toBe(true);

    // Run 2: pending → delayed again
    await new Promise<void>((r) => setTimeout(r, 150));
    await queue.promoteDelayed(jobId);
    try {
      await engine.run(jobId);
    } catch {
      /* intentionally empty */
    }
    expect(svc.callCount).toBe(2);
    expect(queue.isDelayed(jobId)).toBe(true);

    // Run 3: succeeded → workflow completes
    await new Promise<void>((r) => setTimeout(r, 150));
    await queue.promoteDelayed(jobId);
    const result = await engine.run(jobId);
    expect(result).toEqual({ result: 99 });
    expect(svc.callCount).toBe(3);

    const state = (await queue.get(jobId))!.data[DOZER_JOB_STATE_KEY]!;
    expect(state.s).toBe(WORKFLOW_STATUS.completed);
  }, 10_000);

  it('workflow fails when prediction returns failed status', async () => {
    svc.queueResponse({ status: 'failed', error: 'generation-error' });
    const jobId = await engine.start('polling-step-workflow', { id: 'abc' });
    await expect(engine.run(jobId)).rejects.toThrow('generation-error');
  });
});
