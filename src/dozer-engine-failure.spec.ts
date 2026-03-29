import { Injectable, Logger } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import {
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  NonRetryableError,
  Step,
  Workflow,
} from './index';
import { CapturingResultQueue } from './test/workflow-test-utils';
import { DozerWorkflow } from './workflow/dozer-workflow';
import { NoStep } from './decorators/no-step.decorator';

@Injectable()
class OnFailedSpy {
  calls: Array<{ error: Error; input: unknown; jobId: string }> = [];
  throwOnCall = false;
}

@Workflow({ name: 'on-failed-workflow' })
class OnFailedWorkflow extends DozerWorkflow<{ value: number }> {
  constructor(private readonly spy: OnFailedSpy) {
    super();
  }

  @Step({ name: 'fail-step' })
  failStep(): Promise<void> {
    throw new Error('step-on-failed-error');
  }

  async run(): Promise<void> {
    await this.failStep();
  }

  @NoStep()
  onFailed(error: Error, input: { value: number }, jobId: string): void {
    if (this.spy.throwOnCall) {
      throw new Error('on-failed-handler-threw');
    }
    this.spy.calls.push({ error, input, jobId });
  }
}

@Workflow({ name: 'on-failed-non-retryable-workflow' })
class OnFailedNonRetryableWorkflow extends DozerWorkflow<unknown> {
  constructor(private readonly spy: OnFailedSpy) {
    super();
  }

  @Step({ name: 'non-retryable-step' })
  failStep(): Promise<void> {
    throw new NonRetryableError('non-retryable-step-error');
  }

  async run(): Promise<void> {
    await this.failStep();
  }

  @NoStep()
  onFailed(error: Error, input: unknown, jobId: string): void {
    this.spy.calls.push({ error, input, jobId });
  }
}

@Workflow({ name: 'no-on-failed-workflow' })
class NoOnFailedWorkflow extends DozerWorkflow<unknown> {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new Error('no-handler-error');
  }

  async run(): Promise<void> {
    await this.fail();
  }
}

@Workflow({ name: 'global-callback-workflow' })
class GlobalCallbackWorkflow extends DozerWorkflow<unknown> {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new Error('global-callback-error');
  }

  async run(): Promise<void> {
    await this.fail();
  }
}

@Workflow({ name: 'global-callback-non-retryable-workflow' })
class GlobalCallbackNonRetryableWorkflow extends DozerWorkflow<unknown> {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new NonRetryableError('global-nr-error');
  }

  async run(): Promise<void> {
    await this.fail();
  }
}

@Workflow({
  name: 'failure-publish-workflow',
  resultQueue: {
    jobName: 'workflow-result',
    publishOnFailure: true,
  },
})
class FailurePublishWorkflow extends DozerWorkflow<{ value: number }> {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new Error('failure-publish-error');
  }

  async run(): Promise<void> {
    await this.fail();
  }
}

@Workflow({
  name: 'failure-no-publish-workflow',
  resultQueue: {
    jobName: 'workflow-result',
    // publishOnFailure not set — defaults to false
  },
})
class FailureNoPublishWorkflow extends DozerWorkflow<unknown> {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new Error('no-publish-failure-error');
  }

  async run(): Promise<void> {
    await this.fail();
  }
}

@Injectable()
class OnFailedHangSpy {
  hang = false;
}

@Workflow({ name: 'on-failed-hang-workflow' })
class OnFailedHangWorkflow extends DozerWorkflow<unknown> {
  constructor(private readonly spy: OnFailedHangSpy) {
    super();
  }

  @Step({ name: 'fail-hang' })
  failStep(): Promise<void> {
    throw new Error('hang-workflow-error');
  }

  async run(): Promise<void> {
    await this.failStep();
  }

  @NoStep()
  async onFailed(): Promise<void> {
    if (this.spy.hang) {
      await new Promise<void>(() => {
        // intentionally never resolves
      });
    }
  }
}

@Injectable()
class OnFailedStepSpy {
  sideEffect = false;
}

@Workflow({ name: 'on-failed-calls-step-workflow' })
class OnFailedCallsStepWorkflow extends DozerWorkflow<unknown> {
  constructor(private readonly spy: OnFailedStepSpy) {
    super();
  }

  @Step({ name: 'regular-step' })
  regularStep(): void {
    this.spy.sideEffect = true;
  }

  @Step({ name: 'fail-step' })
  failStep(): Promise<void> {
    throw new Error('calls-step-workflow-error');
  }

  async run(): Promise<void> {
    await this.failStep();
  }

  @NoStep()
  onFailed(): void {
    this.regularStep();
  }
}

describe('DozerEngine failure handling', () => {
  it('calls onFailed method with error, input, and jobId on terminal failure', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([OnFailedWorkflow], [OnFailedSpy]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedSpy);
      const jobId = await localEngine.start('on-failed-workflow', {
        value: 42,
      });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'step-on-failed-error',
      );

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0].error.message).toBe('step-on-failed-error');
      expect(spy.calls[0].input).toEqual({ value: 42 });
      expect(spy.calls[0].jobId).toBe(jobId);
    } finally {
      await localModule.close();
    }
  });

  it('suppresses errors thrown inside onFailed and still throws original error', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([OnFailedWorkflow], [OnFailedSpy]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedSpy);
      spy.throwOnCall = true;

      const jobId = await localEngine.start('on-failed-workflow', {
        value: 1,
      });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'step-on-failed-error',
      );
    } finally {
      await localModule.close();
    }
  });

  it('does not crash when workflow has no onFailed method', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([NoOnFailedWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('no-on-failed-workflow', {});
      await expect(localEngine.run(jobId)).rejects.toThrow('no-handler-error');
    } finally {
      await localModule.close();
    }
  });

  it('calls onFailed when NonRetryableError is thrown', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([OnFailedNonRetryableWorkflow], [OnFailedSpy]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedSpy);
      const jobId = await localEngine.start(
        'on-failed-non-retryable-workflow',
        {},
      );

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'non-retryable-step-error',
      );

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0].error.message).toBe('non-retryable-step-error');
    } finally {
      await localModule.close();
    }
  });

  it('calls global onWorkflowFailed callback on terminal failure', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const callbackCalls: Array<{
      jobId: string;
      workflowName: string;
      error: Error;
    }> = [];
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          onWorkflowFailed: (jobId, workflowName, error) => {
            callbackCalls.push({ jobId, workflowName, error });
          },
        }),
        DozerModule.forFeature([GlobalCallbackWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('global-callback-workflow', {});

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'global-callback-error',
      );

      expect(callbackCalls).toHaveLength(1);
      expect(callbackCalls[0].jobId).toBe(jobId);
      expect(callbackCalls[0].workflowName).toBe('global-callback-workflow');
      expect(callbackCalls[0].error.message).toBe('global-callback-error');
    } finally {
      await localModule.close();
    }
  });

  it('suppresses errors thrown inside global onWorkflowFailed callback', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          onWorkflowFailed: () => {
            throw new Error('callback-threw');
          },
        }),
        DozerModule.forFeature([GlobalCallbackWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('global-callback-workflow', {});

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'global-callback-error',
      );
    } finally {
      await localModule.close();
    }
  });

  it('calls global onWorkflowFailed when NonRetryableError is thrown', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const callbackCalls: string[] = [];
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          onWorkflowFailed: (_jobId, _name, error) => {
            callbackCalls.push(error.message);
          },
        }),
        DozerModule.forFeature([GlobalCallbackNonRetryableWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start(
        'global-callback-non-retryable-workflow',
        {},
      );

      await expect(localEngine.run(jobId)).rejects.toThrow('global-nr-error');

      expect(callbackCalls).toEqual(['global-nr-error']);
    } finally {
      await localModule.close();
    }
  });

  it('publishes failure payload to result queue when publishOnFailure is true', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const resultQueue = new CapturingResultQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue, resultQueue }),
        DozerModule.forFeature([FailurePublishWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('failure-publish-workflow', {
        value: 7,
      });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'failure-publish-error',
      );

      expect(resultQueue.added).toHaveLength(1);
      expect(resultQueue.added[0]).toMatchObject({
        name: 'workflow-result',
        data: {
          jobId,
          workflowName: 'failure-publish-workflow',
          status: 'failed',
          result: null,
          error: 'failure-publish-error',
        },
      });
    } finally {
      await localModule.close();
    }
  });

  it('wraps non-Error throw (string) into Error and propagates it', async () => {
    @Workflow({ name: 'throw-string-workflow' })
    class ThrowStringWorkflow extends DozerWorkflow<unknown> {
      @Step({ name: 'throw-string' })
      throwString(): Promise<void> {
        // eslint-disable-next-line @typescript-eslint/only-throw-error
        throw 'plain string error';
      }

      async run(): Promise<void> {
        await this.throwString();
      }
    }

    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([ThrowStringWorkflow]),
      ],
    }).compile();
    await localModule.init();

    const localEngine = localModule.get(DozerEngine);
    const jobId = await localEngine.start('throw-string-workflow', {});
    await expect(localEngine.run(jobId)).rejects.toThrow('plain string error');

    await localModule.close();
  });

  it('wraps non-Error throw (number) into Error and propagates it', async () => {
    @Workflow({ name: 'throw-number-workflow' })
    class ThrowNumberWorkflow extends DozerWorkflow<unknown> {
      @Step({ name: 'throw-number' })
      throwNumber(): Promise<void> {
        // eslint-disable-next-line @typescript-eslint/only-throw-error
        throw 404;
      }

      async run(): Promise<void> {
        await this.throwNumber();
      }
    }

    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([ThrowNumberWorkflow]),
      ],
    }).compile();
    await localModule.init();

    const localEngine = localModule.get(DozerEngine);
    const jobId = await localEngine.start('throw-number-workflow', {});
    await expect(localEngine.run(jobId)).rejects.toThrow('404');

    await localModule.close();
  });

  it('logs error when onFailed throws', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([OnFailedWorkflow], [OnFailedSpy]),
      ],
    }).compile();
    await localModule.init();

    const errorSpy = jest
      .spyOn(Logger.prototype, 'error')
      .mockImplementation(() => undefined);

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedSpy);
      spy.throwOnCall = true;

      const jobId = await localEngine.start('on-failed-workflow', { value: 1 });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'step-on-failed-error',
      );

      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining('on-failed-handler-threw'),
        expect.anything(),
      );
    } finally {
      errorSpy.mockRestore();
      await localModule.close();
    }
  });

  it('times out onFailed and logs error, still throws original error', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: { onFailedTimeoutMs: 50 },
        }),
        DozerModule.forFeature([OnFailedHangWorkflow], [OnFailedHangSpy]),
      ],
    }).compile();
    await localModule.init();

    const errorSpy = jest
      .spyOn(Logger.prototype, 'error')
      .mockImplementation(() => undefined);

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedHangSpy);
      spy.hang = true;

      const jobId = await localEngine.start('on-failed-hang-workflow', {});

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'hang-workflow-error',
      );

      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining('timed out after 50ms'),
        expect.anything(),
      );
    } finally {
      errorSpy.mockRestore();
      await localModule.close();
    }
  });

  it('logs warning when @Step method is called from onFailed', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([OnFailedCallsStepWorkflow], [OnFailedStepSpy]),
      ],
    }).compile();
    await localModule.init();

    const warnSpy = jest
      .spyOn(Logger.prototype, 'warn')
      .mockImplementation(() => undefined);

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedStepSpy);

      const jobId = await localEngine.start(
        'on-failed-calls-step-workflow',
        {},
      );

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'calls-step-workflow-error',
      );

      expect(spy.sideEffect).toBe(true);
      expect(warnSpy).toHaveBeenCalledWith(
        expect.stringContaining('regular-step'),
      );
    } finally {
      warnSpy.mockRestore();
      await localModule.close();
    }
  });

  it('does not publish to result queue on failure when publishOnFailure is false', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const resultQueue = new CapturingResultQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue, resultQueue }),
        DozerModule.forFeature([FailureNoPublishWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('failure-no-publish-workflow', {});

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'no-publish-failure-error',
      );

      expect(resultQueue.added).toHaveLength(0);
    } finally {
      await localModule.close();
    }
  });
});
