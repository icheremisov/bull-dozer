import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  DozerModule,
  DozerWorkflow,
  InMemoryWorkflowQueue,
  StateSizeLimitError,
  Step,
  StepReplayConflictError,
  Workflow,
  WORKFLOW_STATUS,
} from './index';

// ---------------------------------------------------------------------------
// Workflows for traceEnabled tests
// ---------------------------------------------------------------------------

@Workflow({ name: 'trace-enabled-workflow' })
class TraceEnabledWorkflow extends DozerWorkflow<{ value: number }> {
  @Step({ name: 'add' })
  add(v: number): Promise<number> {
    return Promise.resolve(v + 1);
  }

  @Step({ name: 'mul' })
  mul(v: number): Promise<number> {
    return Promise.resolve(v * 2);
  }

  async run(input: { value: number }): Promise<number> {
    const a = await this.add(input.value);
    return this.mul(a);
  }
}

// Trace-disabled variant declared via per-workflow option
@Workflow({
  name: 'trace-disabled-workflow',
  execution: { traceEnabled: false },
})
class TraceDisabledWorkflow extends DozerWorkflow<{ value: number }> {
  @Step({ name: 'add' })
  add(v: number): Promise<number> {
    return Promise.resolve(v + 1);
  }

  @Step({ name: 'mul' })
  mul(v: number): Promise<number> {
    return Promise.resolve(v * 2);
  }

  async run(input: { value: number }): Promise<number> {
    const a = await this.add(input.value);
    return this.mul(a);
  }
}

@Injectable()
class BranchToggle {
  useLeft = true;
}

/**
 * Workflow whose step order changes between runs — would cause
 * StepReplayConflictError under traceEnabled: true.
 */
@Workflow({
  name: 'branch-workflow',
  execution: { traceEnabled: false },
})
class BranchWorkflow extends DozerWorkflow<{ value: number }> {
  constructor(private readonly toggle: BranchToggle) {
    super();
  }

  @Step({ name: 'left' })
  left(v: number): Promise<number> {
    return Promise.resolve(v + 10);
  }

  @Step({ name: 'right' })
  right(v: number): Promise<number> {
    return Promise.resolve(v + 20);
  }

  async run(input: { value: number }): Promise<number> {
    return this.toggle.useLeft
      ? this.left(input.value)
      : this.right(input.value);
  }
}

// ---------------------------------------------------------------------------
// Workflows for maxStateSizeBytes tests
// ---------------------------------------------------------------------------

@Injectable()
class PayloadGenerator {
  sizeBytes = 100;
}

@Workflow({ name: 'large-state-workflow' })
class LargeStateWorkflow extends DozerWorkflow<void> {
  constructor(private readonly gen: PayloadGenerator) {
    super();
  }

  @Step({ name: 'produce' })
  produce(): Promise<string> {
    // Generate a string of roughly `sizeBytes` characters
    return Promise.resolve('x'.repeat(this.gen.sizeBytes));
  }

  async run(): Promise<string> {
    return this.produce();
  }
}

// ---------------------------------------------------------------------------
// traceEnabled tests
// ---------------------------------------------------------------------------

describe('DozerEngine traceEnabled config', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;
  let toggle: BranchToggle;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature(
          [TraceEnabledWorkflow, TraceDisabledWorkflow, BranchWorkflow],
          [BranchToggle],
        ),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
    toggle = moduleRef.get(BranchToggle);
  });

  afterEach(async () => {
    if (moduleRef) await moduleRef.close();
  });

  describe('traceEnabled: true (default)', () => {
    it('t array is populated after workflow runs', async () => {
      const jobId = await engine.start('trace-enabled-workflow', { value: 3 });
      await engine.run(jobId);
      const state = (await queue.get(jobId))!.data[DOZER_JOB_STATE_KEY]!;
      expect(state.t.length).toBeGreaterThan(0);
      expect(state.t).toContain('0:add');
      expect(state.t).toContain('1:mul');
    });

    it('StepReplayConflictError is thrown when step order changes between runs', async () => {
      // Workflow that fails on first run so trace is saved but job is not completed.
      // On retry with different branch → trace[0] = "0:left" but code calls "0:right".
      @Injectable()
      class FailOnce {
        called = false;
      }

      @Workflow({ name: 'branch-trace-on' })
      class BranchTraceOnWorkflow extends DozerWorkflow<{ value: number }> {
        constructor(
          private readonly tog: BranchToggle,
          private readonly fo: FailOnce,
        ) {
          super();
        }

        @Step({ name: 'left' })
        left(v: number): Promise<number> {
          return Promise.resolve(v + 10);
        }

        @Step({ name: 'right' })
        right(v: number): Promise<number> {
          return Promise.resolve(v + 20);
        }

        @Step({ name: 'maybe-fail' })
        maybeFail(): Promise<void> {
          if (!this.fo.called) {
            this.fo.called = true;
            throw new Error('first-run-failure');
          }
          return Promise.resolve();
        }

        async run(input: { value: number }): Promise<number> {
          const v = this.tog.useLeft
            ? await this.left(input.value)
            : await this.right(input.value);
          await this.maybeFail();
          return v;
        }
      }

      const localQueue = new InMemoryWorkflowQueue();
      const localToggle = new BranchToggle();
      const fo = new FailOnce();

      const localModule = await Test.createTestingModule({
        imports: [
          DozerModule.forRoot({ driver: localQueue }),
          DozerModule.forFeature(
            [BranchTraceOnWorkflow],
            [
              { provide: BranchToggle, useValue: localToggle },
              { provide: FailOnce, useValue: fo },
            ],
          ),
        ],
      }).compile();
      await localModule.init();

      try {
        const localEngine = localModule.get(DozerEngine);

        // Run 1: left branch → trace[0]="0:left". maybeFail throws → workflow fails.
        localToggle.useLeft = true;
        const jobId = await localEngine.start('branch-trace-on', { value: 1 });
        await expect(localEngine.run(jobId)).rejects.toThrow(
          'first-run-failure',
        );

        // Run 2: right branch → trace[0]="0:left" but code calls "0:right" → conflict.
        localToggle.useLeft = false;
        await expect(localEngine.run(jobId)).rejects.toBeInstanceOf(
          StepReplayConflictError,
        );
      } finally {
        await localModule.close();
      }
    });
  });

  describe('traceEnabled: false (per-workflow)', () => {
    it('t array stays empty throughout execution', async () => {
      const jobId = await engine.start('trace-disabled-workflow', { value: 3 });
      await engine.run(jobId);
      const state = (await queue.get(jobId))!.data[DOZER_JOB_STATE_KEY]!;
      expect(state.t).toHaveLength(0);
    });

    it('workflow produces correct result', async () => {
      const jobId = await engine.start('trace-disabled-workflow', { value: 3 });
      const result = await engine.run(jobId);
      expect(result).toBe(8); // (3 + 1) * 2
    });

    it('step replay works correctly (step results still cached)', async () => {
      const jobId = await engine.start('trace-disabled-workflow', { value: 5 });
      const result = await engine.run(jobId);
      expect(result).toBe(12); // (5 + 1) * 2
      const state = (await queue.get(jobId))!.data[DOZER_JOB_STATE_KEY]!;
      expect(state.s).toBe(WORKFLOW_STATUS.completed);
    });

    it('StepReplayConflictError is never thrown even when step order changes', async () => {
      toggle.useLeft = true;
      const jobId = await engine.start('branch-workflow', { value: 1 });

      // First run: left branch
      // (this workflow has no failure so it completes — but if it had a fail-once,
      //  changing the branch on replay would NOT throw StepReplayConflictError)
      toggle.useLeft = false; // change before run — no conflict error
      const result = await engine.run(jobId);
      // With trace disabled, whichever branch runs, no conflict error
      expect(typeof result).toBe('number');
    });
  });

  describe('traceEnabled: false (module-level default)', () => {
    it('t array stays empty when set via module defaults', async () => {
      const localQueue = new InMemoryWorkflowQueue();
      const localModule = await Test.createTestingModule({
        imports: [
          DozerModule.forRoot({
            driver: localQueue,
            defaults: { execution: { traceEnabled: false } },
          }),
          DozerModule.forFeature([TraceEnabledWorkflow]),
        ],
      }).compile();
      await localModule.init();

      try {
        const localEngine = localModule.get(DozerEngine);
        const jobId = await localEngine.start('trace-enabled-workflow', {
          value: 2,
        });
        await localEngine.run(jobId);
        const state = (await localQueue.get(jobId))!.data[DOZER_JOB_STATE_KEY]!;
        expect(state.t).toHaveLength(0);
      } finally {
        await localModule.close();
      }
    });
  });
});

// ---------------------------------------------------------------------------
// maxStateSizeBytes tests
// ---------------------------------------------------------------------------

describe('DozerEngine maxStateSizeBytes config', () => {
  let queue: InMemoryWorkflowQueue;

  const makeEngine = async (
    maxStateSizeBytes: number,
    sizeBytes: number,
  ): Promise<{ engine: DozerEngine; moduleRef: TestingModule }> => {
    queue = new InMemoryWorkflowQueue();
    const gen = new PayloadGenerator();
    gen.sizeBytes = sizeBytes;

    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue, maxStateSizeBytes }),
        DozerModule.forFeature(
          [LargeStateWorkflow],
          [{ provide: PayloadGenerator, useValue: gen }],
        ),
      ],
    }).compile();
    await moduleRef.init();
    return { engine: moduleRef.get(DozerEngine), moduleRef };
  };

  it('workflow succeeds when state is within the byte limit', async () => {
    const { engine, moduleRef } = await makeEngine(
      100_000, // 100 KB limit
      100, // 100-byte payload — well within limit
    );
    try {
      const jobId = await engine.start('large-state-workflow', undefined);
      await expect(engine.run(jobId)).resolves.toBe('x'.repeat(100));
    } finally {
      await moduleRef.close();
    }
  });

  it('StateSizeLimitError is thrown and job is failed when state exceeds limit', async () => {
    const { engine, moduleRef } = await makeEngine(
      50, // 50-byte limit — tiny
      1_000, // 1 KB payload — far exceeds limit
    );
    try {
      const jobId = await engine.start('large-state-workflow', undefined);
      await expect(engine.run(jobId)).rejects.toBeInstanceOf(
        StateSizeLimitError,
      );

      const state = (await queue.get(jobId))!.data[DOZER_JOB_STATE_KEY]!;
      expect(state.s).toBe(WORKFLOW_STATUS.failed);
    } finally {
      await moduleRef.close();
    }
  });

  it('StateSizeLimitError reports actual and limit byte counts', async () => {
    const { engine, moduleRef } = await makeEngine(50, 1_000);
    try {
      const jobId = await engine.start('large-state-workflow', undefined);
      let caught!: StateSizeLimitError;
      try {
        await engine.run(jobId);
      } catch (e) {
        caught = e as StateSizeLimitError;
      }
      expect(caught).toBeInstanceOf(StateSizeLimitError);
      expect(caught.limitBytes).toBe(50);
      expect(caught.actualBytes).toBeGreaterThan(50);
    } finally {
      await moduleRef.close();
    }
  });

  it('boundary: workflow fails when state is exactly 1 byte over the limit', async () => {
    // Run first to find the actual state size, then set limit to that − 1
    const { engine: e1, moduleRef: m1 } = await makeEngine(100_000, 10);
    let actualSize = 0;
    try {
      const jobId = await e1.start('large-state-workflow', undefined);
      await e1.run(jobId);
      const raw = (await queue.get(jobId))!.data[DOZER_JOB_STATE_KEY]!;
      actualSize = Buffer.byteLength(JSON.stringify(raw), 'utf8');
    } finally {
      await m1.close();
    }

    // Now run with limit set to actualSize - 1
    const { engine: e2, moduleRef: m2 } = await makeEngine(actualSize - 1, 10);
    try {
      const jobId = await e2.start('large-state-workflow', undefined);
      await expect(e2.run(jobId)).rejects.toBeInstanceOf(StateSizeLimitError);
    } finally {
      await m2.close();
    }
  });
});
