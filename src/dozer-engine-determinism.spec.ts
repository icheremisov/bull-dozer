import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  NonDeterminismError,
  Step,
  StepReplayConflictError,
  Workflow,
  WORKFLOW_STATUS,
} from './index';
import { sleep } from './test/workflow-test-utils';
import { DozerWorkflow } from './workflow/dozer-workflow';

@Injectable()
class NestedReplayStats {
  outer = 0;
  inner = 0;
  fail = 0;
}

@Injectable()
class DeterminismProbeStats {
  computeCalls = 0;
}

@Injectable()
class BranchService {
  branch: 'left' | 'right' = 'left';
}

@Injectable()
class FailOnceServiceLocal {
  private readonly map = new Map<string, number>();

  shouldFail(key: string, failTimes = 1): boolean {
    const current = this.map.get(key) ?? 0;
    this.map.set(key, current + 1);
    return current < failTimes;
  }

  reset(): void {
    this.map.clear();
  }
}

@Workflow({ name: 'nondeterministic-workflow' })
class NonDeterministicWorkflow extends DozerWorkflow<{
  id: string;
  value: number;
}> {
  constructor(
    private readonly branch: BranchService,
    private readonly failOnce: FailOnceServiceLocal,
  ) {
    super();
  }

  @Step({ name: 'left-branch' })
  left(input: number): Promise<number> {
    return Promise.resolve(input + 10);
  }

  @Step({ name: 'right-branch' })
  right(input: number): Promise<number> {
    return Promise.resolve(input + 20);
  }

  @Step({ name: 'fail-once' })
  fail(input: string): Promise<void> {
    if (this.failOnce.shouldFail(input)) {
      throw new Error('fail-once');
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const value =
      this.branch.branch === 'left'
        ? await this.left(input.value)
        : await this.right(input.value);

    await this.fail(input.id);
    return { value };
  }
}

@Workflow({ name: 'nested-replay-workflow' })
class NestedReplayWorkflow extends DozerWorkflow<{
  id: string;
  value: number;
}> {
  constructor(
    private readonly stats: NestedReplayStats,
    private readonly failOnce: FailOnceServiceLocal,
  ) {
    super();
  }

  @Step({ name: 'outer' })
  outer(input: { value: number }): Promise<number> {
    this.stats.outer += 1;
    return this.inner(input.value);
  }

  @Step({ name: 'inner' })
  inner(value: number): Promise<number> {
    this.stats.inner += 1;
    return Promise.resolve(value + 1);
  }

  @Step({ name: 'fail-once' })
  fail(id: string): Promise<void> {
    this.stats.fail += 1;
    if (this.failOnce.shouldFail(`nested-replay:${id}`)) {
      throw new Error('nested-replay-fail-once');
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const value = await this.outer({ value: input.value });
    await this.fail(input.id);
    return { value };
  }
}

@Workflow({
  name: 'determinism-probe-stable-workflow',
  execution: {
    autoDeterminismProbe: true,
    determinismProbeMaxDurationMs: 30,
  },
})
class DeterminismProbeStableWorkflow extends DozerWorkflow<{ value: number }> {
  constructor(private readonly stats: DeterminismProbeStats) {
    super();
  }

  @Step({ name: 'compute' })
  compute(input: { value: number }): Promise<{ value: number }> {
    this.stats.computeCalls += 1;
    return Promise.resolve({ value: input.value + 1 });
  }

  run(input: { value: number }): Promise<{ value: number }> {
    return this.compute(input);
  }
}

@Workflow({
  name: 'determinism-probe-random-workflow',
  execution: {
    autoDeterminismProbe: true,
    determinismProbeMaxDurationMs: 30,
  },
})
class DeterminismProbeRandomWorkflow extends DozerWorkflow<unknown> {
  run(): Promise<{ value: number }> {
    return Promise.resolve({ value: Math.random() });
  }
}

@Workflow({
  name: 'determinism-probe-slow-workflow',
  execution: {
    autoDeterminismProbe: true,
    determinismProbeMaxDurationMs: 1,
  },
})
class DeterminismProbeSlowWorkflow extends DozerWorkflow<{ value: number }> {
  async run(input: { value: number }): Promise<{ value: number }> {
    await sleep(5);
    return { value: input.value + 1 };
  }
}

@Workflow({ name: 'global-determinism-probe-random-workflow' })
class GlobalDeterminismProbeRandomWorkflow extends DozerWorkflow<unknown> {
  run(): Promise<{ value: number }> {
    return Promise.resolve({ value: Math.random() });
  }
}

describe('DozerEngine determinism', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature(
          [
            NonDeterministicWorkflow,
            NestedReplayWorkflow,
            DeterminismProbeStableWorkflow,
            DeterminismProbeRandomWorkflow,
            DeterminismProbeSlowWorkflow,
            GlobalDeterminismProbeRandomWorkflow,
          ],
          [
            NestedReplayStats,
            DeterminismProbeStats,
            BranchService,
            FailOnceServiceLocal,
          ],
        ),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
  });

  afterEach(async () => {
    if (moduleRef) await moduleRef.close();
  });

  it('detects non-deterministic replay', async () => {
    const branch = moduleRef.get(BranchService);
    const failOnce = moduleRef.get(FailOnceServiceLocal);
    failOnce.reset();
    branch.branch = 'left';

    const jobId = await engine.start('nondeterministic-workflow', {
      id: 'x',
      value: 1,
    });

    await expect(engine.run(jobId)).rejects.toThrow('fail-once');

    branch.branch = 'right';

    await expect(engine.run(jobId)).rejects.toBeInstanceOf(
      StepReplayConflictError,
    );
  });

  it('replays cached nested steps without trace conflicts', async () => {
    const stats = moduleRef.get(NestedReplayStats);
    const failOnce = moduleRef.get(FailOnceServiceLocal);
    failOnce.reset();

    const jobId = await engine.start('nested-replay-workflow', {
      id: 'nested-1',
      value: 5,
    });

    await expect(engine.run(jobId)).rejects.toThrow('nested-replay-fail-once');

    const failedJob = await queue.get(jobId);
    const failedState = failedJob?.data[DOZER_JOB_STATE_KEY];
    expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
    expect(failedState?.c['0:outer']).toBe(6);
    expect(failedState?.c['0.0:inner']).toBeUndefined();
    expect(stats.outer).toBe(1);
    expect(stats.inner).toBe(1);
    expect(stats.fail).toBe(1);

    await expect(engine.run(jobId)).resolves.toEqual({ value: 6 });

    const completedJob = await queue.get(jobId);
    const completedState = completedJob?.data[DOZER_JOB_STATE_KEY];
    expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
    expect(completedState?.c['0:outer']).toBe(6);
    expect(completedState?.c['0.0:inner']).toBeUndefined();
    expect(completedState?.t).toContain('0.0:inner');
    expect(stats.outer).toBe(1);
    expect(stats.inner).toBe(1);
    expect(stats.fail).toBe(2);
  });

  it('runs determinism probe after completion and reuses cached step results', async () => {
    const stats = moduleRef.get(DeterminismProbeStats);
    stats.computeCalls = 0;

    const jobId = await engine.start('determinism-probe-stable-workflow', {
      value: 1,
    });
    await expect(engine.run(jobId)).resolves.toEqual({ value: 2 });
    expect(stats.computeCalls).toBe(1);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
  });

  it('fails determinism probe when replayed result diverges', async () => {
    const jobId = await engine.start('determinism-probe-random-workflow', {});

    await expect(engine.run(jobId)).rejects.toBeInstanceOf(NonDeterminismError);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.r).toBeDefined();
    expect(job?.data[DOZER_JOB_STATE_KEY]?.e).toBeUndefined();
  });

  it('fails determinism probe when replay run is too slow', async () => {
    const jobId = await engine.start('determinism-probe-slow-workflow', {
      value: 1,
    });

    await expect(engine.run(jobId)).rejects.toBeInstanceOf(NonDeterminismError);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.r).toBeDefined();
    expect(job?.data[DOZER_JOB_STATE_KEY]?.e).toBeUndefined();
  });

  it('supports module-level defaults for worker determinism probe', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            execution: {
              autoDeterminismProbe: true,
              determinismProbeMaxDurationMs: 30,
            },
          },
        }),
        DozerModule.forFeature([GlobalDeterminismProbeRandomWorkflow]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start(
        'global-determinism-probe-random-workflow',
        {},
      );

      await expect(localEngine.run(jobId)).rejects.toBeInstanceOf(
        NonDeterminismError,
      );
      const job = await localQueue.get(jobId);
      expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
      expect(job?.data[DOZER_JOB_STATE_KEY]?.r).toBeDefined();
    } finally {
      await localModule.close();
    }
  });
});
