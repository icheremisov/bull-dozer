import { Inject, Injectable } from '@nestjs/common';
import type { ConnectionOptions, Queue, Worker, WorkerOptions } from 'bullmq';
import {
  DOZER_JOB_INPUT_KEY,
  DOZER_JOB_STATE_KEY,
  DOZER_MODULE_OPTIONS,
  DOZER_QUEUE_DRIVER,
} from '../constants';
import type { DozerModuleOptions } from '../dozer.module';
import { WORKFLOW_STATUS } from '../queue/workflow-queue';
import type {
  BullMQQueueLike,
  BullMQJobLike,
  WorkflowJobInfo,
  WorkflowJobData,
  WorkflowJobOptions,
  WorkflowResultQueueJobData,
  WorkflowResultQueueJobInfo,
  WorkflowQueueDriver,
} from '../queue/workflow-queue';
import { toWorkflowResultQueueJobId } from '../queue/workflow-queue';
import { WorkflowJobNotFoundError } from '../errors/workflow-job-not-found.error';
import { getWorkflowJobInfo } from '../runtime/workflow-job-info';
import { WorkflowStateStore } from '../runtime/workflow-state.store';
import {
  deserializeFromStorage,
  serializeForStorage,
} from '../runtime/value-serializer';
import {
  createWorkflowResultWorker,
  type WorkflowResultHandler,
} from './workflow-result-worker';

const mergeJobOptions = (
  defaults?: WorkflowJobOptions,
  overrides?: WorkflowJobOptions,
): WorkflowJobOptions | undefined => {
  if (!defaults && !overrides) {
    return undefined;
  }

  return {
    ...(defaults ?? {}),
    ...(overrides ?? {}),
  };
};

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

export interface WaitForWorkflowResultOptions {
  timeoutMs?: number;
  pollMs?: number;
}

type BullMQResultQueueInstance = Queue<WorkflowResultQueueJobData<unknown>> & {
  opts?: {
    connection?: ConnectionOptions;
  };
};

@Injectable()
export class DozerClient {
  constructor(
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
    const mergedOptions = mergeJobOptions(
      this.moduleOptions.defaults?.job,
      jobOptions,
    );

    const job = await this.queue.add<unknown>(
      workflowName,
      jobData,
      mergedOptions,
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

  async hasResult(workflowJobId: string): Promise<boolean> {
    return (await this.getResultJob(workflowJobId)) !== null;
  }

  async getResult<TResult = unknown>(
    workflowJobId: string,
  ): Promise<TResult | null> {
    const resultJob = await this.getResultJob<TResult>(workflowJobId);
    return resultJob?.result ?? null;
  }

  async getResultJob<TResult = unknown>(
    workflowJobId: string,
  ): Promise<WorkflowResultQueueJobInfo<TResult> | null> {
    const resultQueueJob = await this.getResultQueueJob(workflowJobId);
    if (!resultQueueJob) {
      return null;
    }

    const payload = resultQueueJob.data as Partial<
      WorkflowResultQueueJobData<unknown>
    > | null;
    if (
      !payload ||
      typeof payload !== 'object' ||
      typeof payload.jobId !== 'string' ||
      typeof payload.workflowName !== 'string' ||
      !('result' in payload)
    ) {
      throw new Error(
        `Result queue job "${String(resultQueueJob.id ?? workflowJobId)}" has invalid payload.`,
      );
    }

    return {
      id: String(resultQueueJob.id ?? workflowJobId),
      name: resultQueueJob.name,
      jobId: payload.jobId,
      workflowName: payload.workflowName,
      result: deserializeFromStorage(payload.result) as TResult,
    };
  }

  async waitForResult<TResult = unknown>(
    workflowJobId: string,
    options?: WaitForWorkflowResultOptions,
  ): Promise<TResult> {
    const timeoutMs = options?.timeoutMs ?? 15000;
    const pollMs = options?.pollMs ?? 100;
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      const resultJob = await this.getResultJob<TResult>(workflowJobId);
      if (resultJob) {
        return resultJob.result;
      }

      const jobInfo = await this.getJobInfo(workflowJobId);
      if (
        jobInfo.status === WORKFLOW_STATUS.failed ||
        jobInfo.status === WORKFLOW_STATUS.cancelled
      ) {
        throw new Error(
          `Workflow job "${workflowJobId}" finished with status "${jobInfo.statusName}" before result queue payload was available.`,
        );
      }

      await sleep(pollMs);
    }

    throw new Error(
      `Timed out waiting for workflow result queue payload for job "${workflowJobId}" after ${timeoutMs}ms.`,
    );
  }

  createResultWorker<TResult = unknown, TReturn = unknown>(
    handler: WorkflowResultHandler<TResult, TReturn>,
    workerOptions?: Omit<WorkerOptions, 'connection'>,
  ): Worker<WorkflowResultQueueJobData<unknown>, TReturn, string> {
    const resultQueue = this.getBullMqResultQueueInstance();
    const connection = resultQueue.opts?.connection;
    if (!connection) {
      throw new Error(
        'DozerClient.createResultWorker() requires BullMQ resultQueue with accessible queue opts.connection.',
      );
    }

    return createWorkflowResultWorker<TResult, TReturn>({
      queueName: resultQueue.name,
      connection,
      handler,
      worker: workerOptions,
    });
  }

  private getConfiguredResultQueue(): BullMQQueueLike<
    WorkflowResultQueueJobData<unknown>
  > {
    if (!this.moduleOptions.resultQueue) {
      throw new Error(
        'DozerClient result queue methods require "resultQueue" in DozerModule.forRoot/forClient options.',
      );
    }

    return this.moduleOptions.resultQueue;
  }

  private async getResultQueueJob(
    workflowJobId: string,
  ): Promise<BullMQJobLike<unknown> | null> {
    const resultQueue = this.getConfiguredResultQueue();
    const mappedJobId = toWorkflowResultQueueJobId(workflowJobId);
    const mappedJob = await resultQueue.getJob(mappedJobId);
    if (mappedJob) {
      return mappedJob;
    }

    if (mappedJobId === workflowJobId) {
      return null;
    }

    return (await resultQueue.getJob(workflowJobId)) ?? null;
  }

  private getBullMqResultQueueInstance(): BullMQResultQueueInstance {
    const resultQueue = this.getConfiguredResultQueue() as unknown;
    const candidate = resultQueue as Partial<BullMQResultQueueInstance>;
    if (typeof candidate?.name !== 'string') {
      throw new Error(
        'DozerClient.createResultWorker() requires a real BullMQ Queue instance as "resultQueue".',
      );
    }

    return candidate as BullMQResultQueueInstance;
  }
}
