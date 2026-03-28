import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  DOZER_JOB_INPUT_KEY,
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  Step,
  Workflow,
  WorkflowCancelledError,
  WORKFLOW_STATUS,
} from './index';
import { FailOnceService, sleep } from './test/workflow-test-utils';

type RecoveryPayload = Record<string, unknown>;

@Injectable()
class RecoveryStats {
  validate = 0;
  process = 0;
  store = 0;
}

@Injectable()
class BranchService {
  branch: 'left' | 'right' = 'left';
}

@Workflow({ name: 'recovery-workflow' })
class RecoveryWorkflow {
  constructor(private readonly stats: RecoveryStats) {}

  @Step({ name: 'validate' })
  validate(input: RecoveryPayload): Promise<RecoveryPayload> {
    this.stats.validate += 1;
    return Promise.resolve({ ...input, validated: true });
  }

  @Step({ name: 'process' })
  process(input: RecoveryPayload): Promise<RecoveryPayload> {
    this.stats.process += 1;
    if (this.stats.process === 1) {
      throw new Error('transient-process-failure');
    }

    return Promise.resolve({ ...input, processed: true });
  }

  @Step({ name: 'store' })
  store(input: RecoveryPayload): Promise<RecoveryPayload> {
    this.stats.store += 1;
    return Promise.resolve(input);
  }

  async run(
    input: RecoveryPayload,
  ): Promise<{ success: true; payload: RecoveryPayload }> {
    const validated = await this.validate(input);
    const processed = await this.process(validated);
    await this.store(processed);
    return { success: true, payload: processed };
  }
}

@Workflow({ name: 'retry-workflow' })
class RetryWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  @Step({ name: 'unstable', retry: { attempts: 3 } })
  unstable(value: number): Promise<number> {
    if (this.failOnce.shouldFail('retry-workflow', 2)) {
      throw new Error('temporary-error');
    }

    return Promise.resolve(value + 1);
  }

  run(input: { value: number }): Promise<number> {
    return this.unstable(input.value);
  }
}

@Workflow({ name: 'typed-step-workflow' })
class TypedStepWorkflow {
  @Step({ name: 'void-step' })
  doNothing(): Promise<void> {
    return Promise.resolve();
  }

  @Step({ name: 'undefined-step' })
  returnUndefined(): Promise<undefined> {
    return Promise.resolve(undefined);
  }

  @Step({ name: 'value-step' })
  plusOne(value: number): Promise<number> {
    return Promise.resolve(value + 1);
  }

  async run(input: { value: number }): Promise<number> {
    await this.doNothing();
    await this.returnUndefined();
    return this.plusOne(input.value);
  }
}

@Workflow({ name: 'repeated-step-workflow' })
class RepeatedStepWorkflow {
  @Step({ name: 'inc' })
  inc(value: number): Promise<number> {
    return Promise.resolve(value + 1);
  }

  async run(input: { value: number }): Promise<number> {
    const first = await this.inc(input.value);
    return this.inc(first);
  }
}

@Workflow({ name: 'typed-input-workflow' })
class TypedInputWorkflow {
  @Step({ name: 'normalize' })
  normalize(
    input:
      | { kind: 'number'; value: number }
      | { kind: 'string'; value: string }
      | { kind: 'array'; value: string[] },
  ): Promise<string> {
    switch (input.kind) {
      case 'number':
        return Promise.resolve(String(input.value));
      case 'string':
        return Promise.resolve(input.value);
      case 'array':
        return Promise.resolve(input.value.join(','));
    }
  }

  async run(
    input:
      | { kind: 'number'; value: number }
      | { kind: 'string'; value: string }
      | { kind: 'array'; value: string[] },
  ): Promise<{ normalized: string }> {
    return { normalized: await this.normalize(input) };
  }
}

describe('DozerEngine core', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();

    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: queue,
        }),
        DozerModule.forFeature(
          [
            RecoveryWorkflow,
            RetryWorkflow,
            TypedStepWorkflow,
            RepeatedStepWorkflow,
            TypedInputWorkflow,
          ],
          [RecoveryStats, BranchService, FailOnceService],
        ),
      ],
    }).compile();

    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
  });

  afterEach(async () => {
    if (moduleRef) {
      await moduleRef.close();
    }
  });

  it('restores workflow state and replays completed steps only once', async () => {
    const stats = moduleRef.get(RecoveryStats);

    const jobId = await engine.start('recovery-workflow', { orderId: 42 });

    await expect(engine.run(jobId)).rejects.toThrow(
      'transient-process-failure',
    );

    const failedJob = await queue.get(jobId);
    const failedState = failedJob?.data[DOZER_JOB_STATE_KEY];

    expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
    expect(failedState?.c['0:validate']).toBeDefined();
    expect(failedState?.c['1:process']).toBeUndefined();
    expect(stats.validate).toBe(1);
    expect(stats.process).toBe(1);

    const result = await engine.run(jobId);
    expect(result).toEqual({
      success: true,
      payload: { orderId: 42, validated: true, processed: true },
    });

    const completedJob = await queue.get(jobId);
    const completedState = completedJob?.data[DOZER_JOB_STATE_KEY];
    expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
    expect(stats.validate).toBe(1);
    expect(stats.process).toBe(2);
    expect(stats.store).toBe(1);
  });

  it('returns workflow job info with status and result by jobId', async () => {
    const jobId = await engine.start('typed-input-workflow', {
      kind: 'number',
      value: 42,
    });

    await expect(engine.getJobInfo(jobId)).resolves.toMatchObject({
      id: jobId,
      name: 'typed-input-workflow',
      status: WORKFLOW_STATUS.pending,
      statusName: 'pending',
      result: undefined,
    });

    await expect(engine.run(jobId)).resolves.toEqual({ normalized: '42' });

    await expect(engine.getJobInfo(jobId)).resolves.toMatchObject({
      id: jobId,
      name: 'typed-input-workflow',
      status: WORKFLOW_STATUS.completed,
      statusName: 'completed',
      result: { normalized: '42' },
    });
  });

  it('cancels pending workflow job and prevents running it', async () => {
    const jobId = await engine.start('retry-workflow', { value: 1 });

    await expect(engine.cancel(jobId)).resolves.toBe(true);
    await expect(engine.cancel(jobId)).resolves.toBe(false);
    await expect(engine.getJobInfo(jobId)).resolves.toMatchObject({
      id: jobId,
      status: WORKFLOW_STATUS.cancelled,
      statusName: 'cancelled',
    });
    await expect(engine.run(jobId)).rejects.toBeInstanceOf(
      WorkflowCancelledError,
    );
  });

  it('marks state as failed when workflow is not registered', async () => {
    const job = await queue.add('unknown-workflow', {
      [DOZER_JOB_INPUT_KEY]: { any: 'value' },
      [DOZER_JOB_STATE_KEY]: {
        s: WORKFLOW_STATUS.pending,
        c: {},
        t: [],
      },
    });

    await expect(engine.run(job.id)).rejects.toThrow('not registered');

    const failedJob = await queue.get(job.id);
    const failedState = failedJob?.data[DOZER_JOB_STATE_KEY];
    expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
    expect(String(failedState?.e ?? '')).toContain('not registered');
  });

  it('supports steps that return void and undefined', async () => {
    const jobId = await engine.start('typed-step-workflow', { value: 2 });
    const result = await engine.run(jobId);

    expect(result).toBe(3);

    const job = await queue.get(jobId);
    const state = job?.data[DOZER_JOB_STATE_KEY];

    expect(state?.u?.['0:void-step']).toBe(1);
    expect(state?.u?.['1:undefined-step']).toBe(1);
    expect(state?.c['2:value-step']).toBe(3);
  });

  it('handles repeated calls of the same step method as separate step keys', async () => {
    const jobId = await engine.start('repeated-step-workflow', { value: 1 });
    const result = await engine.run(jobId);

    expect(result).toBe(3);

    const job = await queue.get(jobId);
    const state = job?.data[DOZER_JOB_STATE_KEY];
    expect(state?.c['0:inc']).toBe(2);
    expect(state?.c['1:inc']).toBe(3);
  });

  it('supports workflows with different input data types', async () => {
    const numberJob = await engine.start('typed-input-workflow', {
      kind: 'number',
      value: 5,
    });
    const stringJob = await engine.start('typed-input-workflow', {
      kind: 'string',
      value: 'abc',
    });
    const arrayJob = await engine.start('typed-input-workflow', {
      kind: 'array',
      value: ['a', 'b'],
    });

    await expect(engine.run(numberJob)).resolves.toEqual({ normalized: '5' });
    await expect(engine.run(stringJob)).resolves.toEqual({ normalized: 'abc' });
    await expect(engine.run(arrayJob)).resolves.toEqual({ normalized: 'a,b' });
  });
});
