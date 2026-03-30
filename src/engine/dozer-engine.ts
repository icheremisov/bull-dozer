import { isDeepStrictEqual } from 'node:util';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { DelayedError } from 'bullmq';
import {
  DOZER_JOB_INPUT_KEY,
  DOZER_MODULE_OPTIONS,
  DOZER_JOB_STATE_KEY,
  DOZER_QUEUE_DRIVER,
} from '../constants';
import type { DozerModuleOptions } from '../dozer.module';
import { WorkflowExecutionOptions } from '../decorators/workflow.decorator';
import { NonDeterminismError } from '../errors/non-determinism.error';
import { NonRetryableError } from '../errors/non-retryable.error';
import { WorkflowSleepRequestedError } from '../errors/workflow-sleep-requested.error';
import { WorkflowSignalWaitRequestedError } from '../errors/workflow-signal-wait-requested.error';
import { WORKFLOW_STATUS } from '../queue/workflow-queue';
import type {
  WorkflowJobInfo,
  WorkflowJobData,
  WorkflowJobOptions,
  WorkflowJob,
  WorkflowResultQueueJobData,
  WorkflowQueueDriver,
} from '../queue/workflow-queue';
import { toWorkflowResultQueueJobId } from '../queue/workflow-queue';
import { WorkflowJobNotFoundError } from '../errors/workflow-job-not-found.error';
import { WorkflowCancelledError } from '../errors/workflow-cancelled.error';
import {
  WorkflowExecutionContext,
  WorkflowExecutionContextStorage,
} from '../runtime/workflow-execution-context';
import { OnFailedContextStorage } from '../runtime/on-failed-context';
import { getWorkflowJobInfo } from '../runtime/workflow-job-info';
import { WorkflowStateStore } from '../runtime/workflow-state.store';
import {
  deserializeFromStorage,
  serializeForStorage,
} from '../runtime/value-serializer';
import {
  RegisteredWorkflow,
  WorkflowRegistry,
} from '../workflow/workflow-registry';
import { DozerWorkflow } from '../workflow/dozer-workflow';
import { resolveRetryDelayMs } from '../runtime/retry-policy';
import { WorkflowRetryRequestedError } from '../runtime/workflow-retry-requested.error';

class WorkflowResultPublishStageError extends NonRetryableError {
  constructor(cause: unknown) {
    super('Workflow result queue publish failed.', cause);
  }
}

class WorkflowResultFinalizeStageError extends NonRetryableError {
  constructor(cause: unknown) {
    super(
      'Workflow result finalize stage failed after result was persisted.',
      cause,
    );
  }
}

class WorkflowDeterminismProbeStageError extends NonRetryableError {
  constructor(cause: unknown) {
    super('Workflow determinism probe failed after completion.', cause);
  }
}

const DEFAULT_ON_FAILED_TIMEOUT_MS = 30_000;

const withOnFailedTimeout = async (
  promise: Promise<void>,
  timeoutMs: number,
): Promise<void> => {
  let timerId: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timerId = setTimeout(
          () => reject(new Error(`onFailed timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    clearTimeout(timerId);
  }
};

const asThrownError = (value: unknown): Error => {
  if (value instanceof Error) {
    return value;
  }

  return new Error(String(value));
};

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

const isDuplicateJobIdError = (error: unknown): boolean => {
  if (!(error instanceof Error)) {
    return false;
  }

  const message = error.message.toLowerCase();
  return message.includes('already exists') && message.includes('job');
};

const mergeRetry = (
  globalExecution?: WorkflowExecutionOptions,
  workflowExecution?: WorkflowExecutionOptions,
): WorkflowExecutionOptions => {
  const mergeRetryOptions = (
    globalRetry?: WorkflowExecutionOptions['stepRetry'],
    workflowRetry?: WorkflowExecutionOptions['stepRetry'],
  ): WorkflowExecutionOptions['stepRetry'] => {
    const attempts = workflowRetry?.attempts ?? globalRetry?.attempts;
    const delayMs =
      workflowRetry?.delayMs ??
      workflowRetry?.backoffMs ??
      globalRetry?.delayMs ??
      globalRetry?.backoffMs;
    const strategy = workflowRetry?.strategy ?? globalRetry?.strategy;

    if (
      attempts === undefined &&
      delayMs === undefined &&
      strategy === undefined
    ) {
      return undefined;
    }

    return {
      attempts,
      delayMs,
      strategy,
    };
  };

  const stepRetry = mergeRetryOptions(
    globalExecution?.stepRetry,
    workflowExecution?.stepRetry,
  );
  const workflowRetry = mergeRetryOptions(
    globalExecution?.workflowRetry,
    workflowExecution?.workflowRetry,
  );

  return {
    stepRetry,
    workflowRetry,
    autoDeterminismProbe:
      workflowExecution?.autoDeterminismProbe ??
      globalExecution?.autoDeterminismProbe,
    determinismProbeMaxDurationMs:
      workflowExecution?.determinismProbeMaxDurationMs ??
      globalExecution?.determinismProbeMaxDurationMs,
    traceEnabled:
      workflowExecution?.traceEnabled ?? globalExecution?.traceEnabled,
  };
};

const mergeJobOptions = (
  globalJob?: WorkflowJobOptions,
  workflowJob?: WorkflowJobOptions,
): WorkflowJobOptions | undefined => {
  if (!globalJob && !workflowJob) {
    return undefined;
  }

  return {
    ...(globalJob ?? {}),
    ...(workflowJob ?? {}),
  };
};

@Injectable()
export class DozerEngine {
  private readonly logger = new Logger(DozerEngine.name);

  constructor(
    private readonly registry: WorkflowRegistry,
    @Inject(DOZER_MODULE_OPTIONS)
    private readonly moduleOptions: DozerModuleOptions,
    @Inject(DOZER_QUEUE_DRIVER)
    private readonly queue: WorkflowQueueDriver,
  ) {}

  async start<TInput = unknown>(
    workflowName: string,
    input: TInput,
    jobOptions?: WorkflowJobOptions,
  ): Promise<string> {
    const workflowDefinition =
      this.registry.resolveOptionalDefinition(workflowName);
    const resolvedJobOptions = mergeJobOptions(
      mergeJobOptions(
        this.moduleOptions.defaults?.job,
        workflowDefinition?.options.job,
      ),
      jobOptions,
    );

    const serializedInput = await serializeForStorage(input, 'workflow input');

    const jobData: WorkflowJobData<unknown> = {
      [DOZER_JOB_INPUT_KEY]: serializedInput,
      [DOZER_JOB_STATE_KEY]: {
        s: WORKFLOW_STATUS.pending,
        c: {},
        a: {},
        t: [],
      },
    };

    const job = await this.queue.add<unknown>(
      workflowName,
      jobData,
      resolvedJobOptions,
    );
    return job.id;
  }

  async getJobInfo<TResult = unknown>(
    jobId: string,
  ): Promise<WorkflowJobInfo<TResult>> {
    const job = await this.queue.get(jobId);
    if (!job) {
      throw new WorkflowJobNotFoundError(jobId);
    }

    return getWorkflowJobInfo<TResult>(job);
  }

  async cancel(jobId: string): Promise<boolean> {
    const job = await this.queue.get(jobId);
    if (!job) {
      throw new WorkflowJobNotFoundError(jobId);
    }

    const jobInfo = getWorkflowJobInfo(job);
    if (
      jobInfo.status !== WORKFLOW_STATUS.pending &&
      jobInfo.status !== WORKFLOW_STATUS.cancelled
    ) {
      return false;
    }
    if (jobInfo.status === WORKFLOW_STATUS.cancelled) {
      return false;
    }

    const stateStore = new WorkflowStateStore(job);
    await stateStore.markCancelled();
    return true;
  }

  private async enqueueWorkflowResult(
    job: WorkflowJob<unknown>,
    definition: RegisteredWorkflow,
    result: unknown,
    failureError?: Error,
  ): Promise<void> {
    const resultQueueOptions = definition.options.resultQueue;
    const resultQueue = this.moduleOptions.resultQueue;
    if (!resultQueueOptions || !resultQueue) {
      return;
    }

    const isFailure = failureError !== undefined;
    const payload: WorkflowResultQueueJobData<unknown> = {
      jobId: job.id,
      workflowName: job.name,
      status: isFailure ? 'failed' : 'completed',
      result: isFailure
        ? null
        : await serializeForStorage(result, 'workflow result queue payload'),
      ...(isFailure ? { error: failureError.message } : {}),
    };
    const resultJobName = resultQueueOptions.jobName ?? `${job.name}:result`;
    const resultQueueJobId = toWorkflowResultQueueJobId(job.id);
    const resultJobOptions: WorkflowJobOptions = {
      ...(resultQueueOptions.job ?? {}),
      jobId: resultQueueJobId,
    };

    try {
      await resultQueue.add(resultJobName, payload, resultJobOptions);
      return;
    } catch (error) {
      const resultJobId = resultQueueJobId;
      if (
        isDuplicateJobIdError(error) &&
        (await resultQueue.getJob(resultJobId))
      ) {
        return;
      }

      throw new WorkflowResultPublishStageError(error);
    }
  }

  private shouldPublishWorkflowResult(definition: RegisteredWorkflow): boolean {
    return Boolean(
      definition.options.resultQueue && this.moduleOptions.resultQueue,
    );
  }

  private async finalizePublishingResult(
    job: WorkflowJob<unknown>,
    definition: RegisteredWorkflow,
  ): Promise<unknown> {
    const stateStore = new WorkflowStateStore(job);
    const serializedResult = stateStore.getFinalResultSerialized();
    const result = deserializeFromStorage(serializedResult);

    try {
      await this.enqueueWorkflowResult(job, definition, result);
      await stateStore.markCompletedFromStoredResult();
    } catch (error) {
      if (error instanceof WorkflowResultPublishStageError) {
        throw error;
      }

      throw new WorkflowResultFinalizeStageError(error);
    }
    return result;
  }

  private async runDeterminismProbe(
    job: WorkflowJob<unknown>,
    definition: RegisteredWorkflow,
    executionOptions: WorkflowExecutionOptions,
  ): Promise<void> {
    if (!executionOptions.autoDeterminismProbe) {
      return;
    }
    try {
      const probeStartedAt = Date.now();
      const probeStateStore = new WorkflowStateStore(job, {
        strictTrace: true,
        readOnly: true,
      });
      const probeContext = new WorkflowExecutionContext(probeStateStore, {
        defaultRetry: executionOptions.stepRetry,
        requireCachedSteps: true,
      });
      const probeWorkflow = this.registry.instantiate(definition);
      const input = deserializeFromStorage(job.data[DOZER_JOB_INPUT_KEY]);
      const probeResult = await WorkflowExecutionContextStorage.run(
        probeContext,
        () => probeWorkflow.run(input),
      );

      probeStateStore.assertTraceConsumed(probeContext.getTraceCursor());

      const maxDurationMs = Math.max(
        1,
        executionOptions.determinismProbeMaxDurationMs ?? 25,
      );
      const elapsedMs = Date.now() - probeStartedAt;
      if (elapsedMs > maxDurationMs) {
        throw new NonDeterminismError(
          `Determinism probe exceeded max duration: ${elapsedMs}ms > ${maxDurationMs}ms.`,
        );
      }

      const serializedProbeResult = await serializeForStorage(
        probeResult,
        'determinism probe result',
      );
      const persistedSerializedResult =
        probeStateStore.getFinalResultSerialized();
      if (
        !isDeepStrictEqual(serializedProbeResult, persistedSerializedResult)
      ) {
        throw new NonDeterminismError(
          'Determinism probe detected result mismatch between cached run and replay run.',
        );
      }
    } catch (error) {
      throw new WorkflowDeterminismProbeStageError(error);
    }
  }

  private async handleWorkflowFailure(
    job: WorkflowJob<unknown>,
    definition: RegisteredWorkflow,
    workflow: unknown,
    input: unknown,
    error: Error,
  ): Promise<void> {
    const onFailed = (workflow as Record<string, unknown> | undefined)
      ?.onFailed;
    if (typeof onFailed === 'function') {
      const timeoutMs =
        this.moduleOptions.defaults?.onFailedTimeoutMs ??
        DEFAULT_ON_FAILED_TIMEOUT_MS;
      try {
        const promise: Promise<void> = OnFailedContextStorage.run(
          () =>
            (
              onFailed as (e: Error, i: unknown, id: string) => Promise<void>
            ).call(workflow, error, input, job.id) as Promise<void>,
        );
        if (timeoutMs > 0) {
          await withOnFailedTimeout(promise, timeoutMs);
        } else {
          await promise;
        }
      } catch (handlerError) {
        this.logger.error(
          `onFailed handler for workflow "${job.name}" (job ${job.id}) failed: ${(handlerError as Error).message}`,
          (handlerError as Error).stack,
        );
      }
    }

    const globalCallback = this.moduleOptions.onWorkflowFailed;
    if (globalCallback) {
      try {
        await globalCallback(job.id, job.name, error);
      } catch {
        // suppressed
      }
    }

    const shouldPublishFailure =
      definition.options.resultQueue?.publishOnFailure === true &&
      Boolean(this.moduleOptions.resultQueue);
    if (shouldPublishFailure) {
      try {
        await this.enqueueWorkflowResult(job, definition, null, error);
      } catch {
        // suppressed
      }
    }
  }

  private resolveDefaultSignalTimeoutMs(): number {
    return (
      this.moduleOptions.defaults?.signalTimeoutMs ?? 7 * 24 * 60 * 60 * 1000
    );
  }

  async run(jobId: string, token?: string): Promise<unknown> {
    const job = await this.queue.get(jobId);
    if (!job) {
      throw new WorkflowJobNotFoundError(jobId);
    }
    if (getWorkflowJobInfo(job).status === WORKFLOW_STATUS.cancelled) {
      throw new WorkflowCancelledError(jobId);
    }

    let definition: RegisteredWorkflow | undefined;
    let executionOptions: WorkflowExecutionOptions = {};
    let maxWorkflowAttempts = 1;
    let workflowRetryBaseDelayMs = 0;
    let workflowRetryStrategy: 'constant' | 'linear' | 'exponential' =
      'constant';
    let workflowFailedAttempts = 0;

    let lastWorkflow: unknown;
    let lastInput: unknown;

    while (true) {
      try {
        if (!definition) {
          definition = this.registry.resolveDefinition(job.name);
          executionOptions = mergeRetry(
            this.moduleOptions.defaults?.execution,
            definition.options.execution,
          );
          const workflowRetryPolicy = executionOptions.workflowRetry;
          maxWorkflowAttempts = Math.max(1, workflowRetryPolicy?.attempts ?? 1);
          workflowRetryBaseDelayMs =
            workflowRetryPolicy?.delayMs ?? workflowRetryPolicy?.backoffMs ?? 0;
          workflowRetryStrategy = workflowRetryPolicy?.strategy ?? 'constant';
        }

        if (getWorkflowJobInfo(job).status === WORKFLOW_STATUS.completing) {
          const result = await this.finalizePublishingResult(job, definition);
          await this.runDeterminismProbe(job, definition, executionOptions);
          return result;
        }

        const stateStore = new WorkflowStateStore(job, {
          traceEnabled: executionOptions.traceEnabled,
          maxStateSizeBytes: this.moduleOptions.maxStateSizeBytes,
        });
        const executionContext = new WorkflowExecutionContext(stateStore, {
          defaultRetry: executionOptions.stepRetry,
        });
        await stateStore.markRunning();
        const workflow = this.registry.instantiate(definition);
        if (workflow instanceof DozerWorkflow) {
          workflow._setJobContext(job);
        }
        lastWorkflow = workflow;
        const input = deserializeFromStorage(job.data[DOZER_JOB_INPUT_KEY]);
        lastInput = input;
        const result = await WorkflowExecutionContextStorage.run(
          executionContext,
          () => workflow.run(input),
        );

        stateStore.assertTraceConsumed(executionContext.getTraceCursor());
        if (this.shouldPublishWorkflowResult(definition)) {
          await stateStore.markPublishingResult(result);
          await this.finalizePublishingResult(job, definition);
        } else {
          await stateStore.markCompleted(result);
        }

        await this.runDeterminismProbe(job, definition, executionOptions);
        return result;
      } catch (error) {
        if (error instanceof DelayedError) throw error;

        if (error instanceof WorkflowResultPublishStageError) {
          throw asThrownError(error.causeError ?? error);
        }

        if (error instanceof WorkflowResultFinalizeStageError) {
          throw asThrownError(error.causeError ?? error);
        }

        if (error instanceof WorkflowDeterminismProbeStageError) {
          throw asThrownError(error.causeError ?? error);
        }

        if (error instanceof WorkflowSleepRequestedError) {
          await this.queue.moveToDelayed(jobId, error.wakeUpAt, token);
          throw new DelayedError();
        }

        if (error instanceof WorkflowSignalWaitRequestedError) {
          const deadline =
            error.expiresAt ??
            Date.now() + this.resolveDefaultSignalTimeoutMs();
          await this.queue.moveToDelayed(jobId, deadline, token);
          throw new DelayedError();
        }

        if (error instanceof WorkflowRetryRequestedError) {
          if (error.backoffMs > 0) {
            await sleep(error.backoffMs);
          }
          continue;
        }

        if (!(error instanceof NonRetryableError) && definition) {
          workflowFailedAttempts += 1;
          if (workflowFailedAttempts < maxWorkflowAttempts) {
            const backoffMs = resolveRetryDelayMs(
              workflowRetryBaseDelayMs,
              workflowRetryStrategy,
              workflowFailedAttempts,
            );
            if (backoffMs > 0) {
              await sleep(backoffMs);
            }
            continue;
          }
        }

        const stateStore = new WorkflowStateStore(job);
        await stateStore.markFailed(error);
        if (definition) {
          await this.handleWorkflowFailure(
            job,
            definition,
            lastWorkflow,
            lastInput,
            asThrownError(error),
          );
        }
        throw asThrownError(error);
      }
    }
  }
}
