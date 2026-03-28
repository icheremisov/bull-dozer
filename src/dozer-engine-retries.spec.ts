import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  NonRetryableError,
  Step,
  TimeoutError,
  Workflow,
  WORKFLOW_STATUS,
  WorkflowJobOptions,
} from './index';
import { FailOnceService, RetryWorkflow, sleep } from './test/workflow-test-utils';
import { DozerWorkflow } from './workflow/dozer-workflow';

@Injectable()
class TimeoutCompensationStats {
  timedOut = 0;
  cleanup = 0;
}

@Injectable()
class WorkflowAutoResumeStats {
  prepare = 0;
  unstable = 0;
}

@Workflow({ name: 'non-retryable-step-workflow' })
class NonRetryableStepWorkflow extends DozerWorkflow<{ id: string; amount: number }> {
  constructor(private readonly failOnce: FailOnceService) {
    super();
  }

  @Step({
    name: 'validate-minimum',
    retry: {
      attempts: 5,
    },
  })
  validateMinimum(input: { id: string; amount: number }): Promise<boolean> {
    if (this.failOnce.shouldFail(`non-retryable:${input.id}`, 1)) {
      throw new NonRetryableError('Amount below minimum');
    }

    return Promise.resolve(input.amount >= 100);
  }

  run(input: { id: string; amount: number }): Promise<boolean> {
    return this.validateMinimum(input);
  }
}

@Workflow({ name: 'timeout-compensation-workflow' })
class TimeoutCompensationWorkflow extends DozerWorkflow<unknown> {
  constructor(private readonly stats: TimeoutCompensationStats) {
    super();
  }

  @Step({
    name: 'process-order',
    timeout: 5,
  })
  async processOrder(): Promise<void> {
    this.stats.timedOut += 1;
    await sleep(20);
  }

  @Step({ name: 'cancel-order' })
  cancelOrder(): Promise<void> {
    this.stats.cleanup += 1;
    return Promise.resolve();
  }

  async run(): Promise<{ compensated: boolean }> {
    try {
      await this.processOrder();
      return { compensated: false };
    } catch (error) {
      if (error instanceof TimeoutError) {
        await this.cancelOrder();
        return { compensated: true };
      }
      throw error;
    }
  }
}

@Workflow({
  name: 'workflow-auto-resume-workflow',
  execution: {
    workflowRetry: {
      attempts: 2,
      delayMs: 1,
      strategy: 'constant',
    },
  },
})
class WorkflowAutoResumeWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(
    private readonly failOnce: FailOnceService,
    private readonly stats: WorkflowAutoResumeStats,
  ) {
    super();
  }

  @Step({ name: 'prepare' })
  prepare(value: number): Promise<number> {
    this.stats.prepare += 1;
    return Promise.resolve(value + 1);
  }

  @Step({ name: 'unstable' })
  unstable(id: string): Promise<void> {
    this.stats.unstable += 1;
    if (this.failOnce.shouldFail(`workflow-auto-resume:${id}`, 1)) {
      return Promise.reject(new Error('workflow-auto-resume-fail-once'));
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const prepared = await this.prepare(input.value);
    await this.unstable(input.id);
    return { value: prepared };
  }
}

@Workflow({
  name: 'workflow-retry-linear-workflow',
  execution: {
    workflowRetry: {
      attempts: 3,
      delayMs: 2,
      strategy: 'linear',
    },
  },
})
class WorkflowRetryLinearWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(private readonly failOnce: FailOnceService) {
    super();
  }

  run(input: { id: string; value: number }): Promise<{ value: number }> {
    if (this.failOnce.shouldFail(`workflow-retry-linear:${input.id}`, 2)) {
      return Promise.reject(new Error('workflow-retry-linear-fail'));
    }

    return Promise.resolve({ value: input.value + 1 });
  }
}

@Workflow({
  name: 'workflow-default-retry-workflow',
  execution: {
    stepRetry: {
      attempts: 2,
    },
  },
})
class WorkflowDefaultRetryWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(private readonly failOnce: FailOnceService) {
    super();
  }

  @Step({ name: 'unstable' })
  unstable(input: { id: string; value: number }): Promise<number> {
    if (this.failOnce.shouldFail(`workflow-default-retry:${input.id}`)) {
      throw new Error('workflow-default-retry-fail-once');
    }

    return Promise.resolve(input.value + 1);
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.unstable(input);
  }
}

@Workflow({ name: 'retry-restarts-whole-flow-workflow' })
class RetryRestartsWholeFlowWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  private localCounter = 0;

  constructor(private readonly failOnce: FailOnceService) {
    super();
  }

  @Step({ name: 'mutating-step', retry: { attempts: 2 } })
  mutatingStep(input: { id: string; value: number }): Promise<number> {
    this.localCounter += 1;

    if (this.failOnce.shouldFail(`retry-restart:${input.id}`)) {
      throw new Error('retry-restart-fail-once');
    }

    if (this.localCounter !== 1) {
      throw new Error('workflow-local-state-corrupted');
    }

    return Promise.resolve(input.value + 1);
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.mutatingStep(input);
  }
}

@Workflow({ name: 'global-default-retry-workflow' })
class GlobalDefaultRetryWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(private readonly failOnce: FailOnceService) {
    super();
  }

  @Step({ name: 'unstable' })
  unstable(input: { id: string; value: number }): Promise<number> {
    if (this.failOnce.shouldFail(`global-default-retry:${input.id}`)) {
      throw new Error('global-default-retry-fail-once');
    }

    return Promise.resolve(input.value + 1);
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.unstable(input);
  }
}

@Workflow({ name: 'global-default-retry-override-workflow' })
class GlobalDefaultRetryOverrideWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(private readonly failOnce: FailOnceService) {
    super();
  }

  @Step({
    name: 'unstable',
    retry: {
      attempts: 1,
    },
  })
  unstable(input: { id: string; value: number }): Promise<number> {
    if (this.failOnce.shouldFail(`global-default-retry-override:${input.id}`)) {
      throw new Error('global-default-retry-override-fail-once');
    }

    return Promise.resolve(input.value + 1);
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.unstable(input);
  }
}

@Workflow({
  name: 'job-options-workflow',
  job: {
    attempts: 2,
    removeOnComplete: true,
  },
})
class JobOptionsWorkflow extends DozerWorkflow<{ value: number }> {
  run(input: { value: number }): Promise<number> {
    return Promise.resolve(input.value);
  }
}

@Workflow({ name: 'global-workflow-retry-workflow' })
class GlobalWorkflowRetryWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(private readonly failOnce: FailOnceService) {
    super();
  }

  run(input: { id: string; value: number }): Promise<{ value: number }> {
    if (this.failOnce.shouldFail(`global-workflow-retry:${input.id}`, 1)) {
      return Promise.reject(new Error('global-workflow-retry-fail-once'));
    }

    return Promise.resolve({ value: input.value + 1 });
  }
}

@Workflow({
  name: 'global-workflow-retry-override-workflow',
  execution: {
    workflowRetry: {
      attempts: 1,
    },
  },
})
class GlobalWorkflowRetryOverrideWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(private readonly failOnce: FailOnceService) {
    super();
  }

  run(input: { id: string; value: number }): Promise<{ value: number }> {
    if (
      this.failOnce.shouldFail(`global-workflow-retry-override:${input.id}`, 1)
    ) {
      return Promise.reject(
        new Error('global-workflow-retry-override-fail-once'),
      );
    }

    return Promise.resolve({ value: input.value + 1 });
  }
}

describe('DozerEngine retries', () => {
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
            RetryWorkflow,
            NonRetryableStepWorkflow,
            TimeoutCompensationWorkflow,
            WorkflowAutoResumeWorkflow,
            WorkflowRetryLinearWorkflow,
            WorkflowDefaultRetryWorkflow,
            RetryRestartsWholeFlowWorkflow,
          ],
          [FailOnceService, TimeoutCompensationStats, WorkflowAutoResumeStats],
        ),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
  });

  afterEach(async () => {
    if (moduleRef) await moduleRef.close();
  });

  it('retries unstable steps by retry policy', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('retry-workflow', { value: 1 });
    const result = await engine.run(jobId);
    expect(result).toBe(2);
    expect(failOnce.calls('retry-workflow')).toBe(3);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
  });

  it('does not retry step when NonRetryableError is thrown', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('non-retryable-step-workflow', {
      id: 'non-retryable-1',
      amount: 10,
    });

    await expect(engine.run(jobId)).rejects.toBeInstanceOf(NonRetryableError);
    expect(failOnce.calls('non-retryable:non-retryable-1')).toBe(1);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.failed);
  });

  it('supports timeout handling with compensating actions in workflow run', async () => {
    const stats = moduleRef.get(TimeoutCompensationStats);
    stats.timedOut = 0;
    stats.cleanup = 0;

    const jobId = await engine.start('timeout-compensation-workflow', {});
    await expect(engine.run(jobId)).resolves.toEqual({ compensated: true });

    expect(stats.timedOut).toBe(1);
    expect(stats.cleanup).toBe(1);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.u?.['0:process-order']).toBe(
      undefined,
    );
    expect(job?.data[DOZER_JOB_STATE_KEY]?.u?.['1:cancel-order']).toBe(1);
  });

  it('automatically resumes workflow by workflowRetry settings', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    const stats = moduleRef.get(WorkflowAutoResumeStats);
    failOnce.reset();
    stats.prepare = 0;
    stats.unstable = 0;

    const jobId = await engine.start('workflow-auto-resume-workflow', {
      id: 'workflow-auto-resume-1',
      value: 5,
    });

    await expect(engine.run(jobId)).resolves.toEqual({ value: 6 });
    expect(stats.prepare).toBe(1);
    expect(stats.unstable).toBe(2);
    expect(failOnce.calls('workflow-auto-resume:workflow-auto-resume-1')).toBe(
      2,
    );

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
  });

  it('applies workflow retry backoff strategy delays', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('workflow-retry-linear-workflow', {
      id: 'workflow-retry-linear-1',
      value: 4,
    });

    const startedAt = Date.now();
    await expect(engine.run(jobId)).resolves.toEqual({ value: 5 });
    const elapsedMs = Date.now() - startedAt;

    expect(elapsedMs).toBeGreaterThanOrEqual(5);
    expect(
      failOnce.calls('workflow-retry-linear:workflow-retry-linear-1'),
    ).toBe(3);
  });

  it('applies workflow-level default retry options for steps without own retry', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('workflow-default-retry-workflow', {
      id: 'workflow-default-retry-1',
      value: 3,
    });
    await expect(engine.run(jobId)).resolves.toBe(4);
    expect(
      failOnce.calls('workflow-default-retry:workflow-default-retry-1'),
    ).toBe(2);
  });

  it('restarts whole workflow on step retry using a fresh workflow instance', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('retry-restarts-whole-flow-workflow', {
      id: 'retry-restart-1',
      value: 10,
    });
    await expect(engine.run(jobId)).resolves.toBe(11);
    expect(failOnce.calls('retry-restart:retry-restart-1')).toBe(2);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
  });

  it('applies module-level default retry options for steps', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            execution: {
              stepRetry: {
                attempts: 2,
              },
            },
          },
        }),
        DozerModule.forFeature([GlobalDefaultRetryWorkflow], [FailOnceService]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const failOnce = localModule.get(FailOnceService);
      failOnce.reset();

      const jobId = await localEngine.start('global-default-retry-workflow', {
        id: 'global-default-retry-1',
        value: 5,
      });
      await expect(localEngine.run(jobId)).resolves.toBe(6);
      expect(
        failOnce.calls('global-default-retry:global-default-retry-1'),
      ).toBe(2);
    } finally {
      await localModule.close();
    }
  });

  it('lets step-level retry options override module defaults', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            execution: {
              stepRetry: {
                attempts: 3,
              },
            },
          },
        }),
        DozerModule.forFeature(
          [GlobalDefaultRetryOverrideWorkflow],
          [FailOnceService],
        ),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const failOnce = localModule.get(FailOnceService);
      failOnce.reset();

      const jobId = await localEngine.start(
        'global-default-retry-override-workflow',
        {
          id: 'global-default-retry-override-1',
          value: 5,
        },
      );

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'global-default-retry-override-fail-once',
      );
      expect(
        failOnce.calls(
          'global-default-retry-override:global-default-retry-override-1',
        ),
      ).toBe(1);

      await expect(localEngine.run(jobId)).resolves.toBe(6);
      expect(
        failOnce.calls(
          'global-default-retry-override:global-default-retry-override-1',
        ),
      ).toBe(2);
    } finally {
      await localModule.close();
    }
  });

  it('applies module-level default workflowRetry options', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            execution: {
              workflowRetry: {
                attempts: 2,
                delayMs: 1,
                strategy: 'constant',
              },
            },
          },
        }),
        DozerModule.forFeature(
          [GlobalWorkflowRetryWorkflow],
          [FailOnceService],
        ),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const failOnce = localModule.get(FailOnceService);
      failOnce.reset();

      const jobId = await localEngine.start('global-workflow-retry-workflow', {
        id: 'global-workflow-retry-1',
        value: 3,
      });
      await expect(localEngine.run(jobId)).resolves.toEqual({ value: 4 });
      expect(
        failOnce.calls('global-workflow-retry:global-workflow-retry-1'),
      ).toBe(2);
    } finally {
      await localModule.close();
    }
  });

  it('lets workflow-level workflowRetry options override module defaults', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            execution: {
              workflowRetry: {
                attempts: 3,
              },
            },
          },
        }),
        DozerModule.forFeature(
          [GlobalWorkflowRetryOverrideWorkflow],
          [FailOnceService],
        ),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const failOnce = localModule.get(FailOnceService);
      failOnce.reset();

      const jobId = await localEngine.start(
        'global-workflow-retry-override-workflow',
        {
          id: 'global-workflow-retry-override-1',
          value: 3,
        },
      );

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'global-workflow-retry-override-fail-once',
      );
      expect(
        failOnce.calls(
          'global-workflow-retry-override:global-workflow-retry-override-1',
        ),
      ).toBe(1);

      await expect(localEngine.run(jobId)).resolves.toEqual({ value: 4 });
      expect(
        failOnce.calls(
          'global-workflow-retry-override:global-workflow-retry-override-1',
        ),
      ).toBe(2);
    } finally {
      await localModule.close();
    }
  });

  it('merges global and workflow job options when creating jobs', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            job: {
              attempts: 5,
              removeOnFail: 100,
            },
          },
        }),
        DozerModule.forFeature([JobOptionsWorkflow]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('job-options-workflow', {
        value: 10,
      });
      const job = await localQueue.get(jobId);

      expect(job?.options).toEqual({
        attempts: 2,
        removeOnFail: 100,
        removeOnComplete: true,
      });

      const unknownJobId = await localEngine.start('unknown-workflow', {
        value: 10,
      });
      const unknownJob = await localQueue.get(unknownJobId);
      expect(unknownJob?.options).toEqual({
        attempts: 5,
        removeOnFail: 100,
      });
    } finally {
      await localModule.close();
    }
  });
});
