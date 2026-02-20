import { isDeepStrictEqual } from 'node:util';
import { Inject, Injectable } from '@nestjs/common';
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
import { WORKFLOW_STATUS } from '../queue/workflow-queue';
import type {
  WorkflowJobData,
  WorkflowJobOptions,
  WorkflowJob,
  WorkflowQueueDriver,
} from '../queue/workflow-queue';
import { WorkflowJobNotFoundError } from '../errors/workflow-job-not-found.error';
import {
  WorkflowExecutionContext,
  WorkflowExecutionContextStorage,
} from '../runtime/workflow-execution-context';
import { WorkflowStateStore } from '../runtime/workflow-state.store';
import {
  deserializeFromStorage,
  serializeForStorage,
} from '../runtime/value-serializer';
import {
  RegisteredWorkflow,
  WorkflowRegistry,
} from '../workflow/workflow-registry';
import { resolveRetryDelayMs } from '../runtime/retry-policy';
import { WorkflowRetryRequestedError } from '../runtime/workflow-retry-requested.error';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
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
  ): Promise<string> {
    const workflowDefinition =
      this.registry.resolveOptionalDefinition(workflowName);
    const jobOptions = mergeJobOptions(
      this.moduleOptions.defaults?.job,
      workflowDefinition?.options.job,
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
      jobOptions,
    );
    return job.id;
  }

  private async runDeterminismProbe(
    job: WorkflowJob<unknown>,
    definition: RegisteredWorkflow,
    executionOptions: WorkflowExecutionOptions,
  ): Promise<void> {
    if (!executionOptions.autoDeterminismProbe) {
      return;
    }

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
    if (!isDeepStrictEqual(serializedProbeResult, persistedSerializedResult)) {
      throw new NonDeterminismError(
        'Determinism probe detected result mismatch between cached run and replay run.',
      );
    }
  }

  async run(jobId: string): Promise<unknown> {
    const job = await this.queue.get(jobId);
    if (!job) {
      throw new WorkflowJobNotFoundError(jobId);
    }

    let definition: RegisteredWorkflow | undefined;
    let executionOptions: WorkflowExecutionOptions = {};
    let maxWorkflowAttempts = 1;
    let workflowRetryBaseDelayMs = 0;
    let workflowRetryStrategy: 'constant' | 'linear' | 'exponential' =
      'constant';
    let workflowFailedAttempts = 0;

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

        const stateStore = new WorkflowStateStore(job);
        const executionContext = new WorkflowExecutionContext(stateStore, {
          defaultRetry: executionOptions.stepRetry,
        });
        await stateStore.markRunning();
        const workflow = this.registry.instantiate(definition);
        const input = deserializeFromStorage(job.data[DOZER_JOB_INPUT_KEY]);
        const result = await WorkflowExecutionContextStorage.run(
          executionContext,
          () => workflow.run(input),
        );

        stateStore.assertTraceConsumed(executionContext.getTraceCursor());
        await stateStore.markCompleted(result);
        await this.runDeterminismProbe(job, definition, executionOptions);
        return result;
      } catch (error) {
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
        throw error;
      }
    }
  }
}
