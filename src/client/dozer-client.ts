import { Inject, Injectable } from '@nestjs/common';
import {
  DOZER_JOB_INPUT_KEY,
  DOZER_JOB_STATE_KEY,
  DOZER_MODULE_OPTIONS,
  DOZER_QUEUE_DRIVER,
} from '../constants';
import type { DozerModuleOptions } from '../dozer.module';
import { WORKFLOW_STATUS } from '../queue/workflow-queue';
import type {
  WorkflowJobInfo,
  WorkflowJobData,
  WorkflowJobOptions,
  WorkflowQueueDriver,
} from '../queue/workflow-queue';
import { WorkflowJobNotFoundError } from '../errors/workflow-job-not-found.error';
import { getWorkflowJobInfo } from '../runtime/workflow-job-info';
import { WorkflowStateStore } from '../runtime/workflow-state.store';
import { serializeForStorage } from '../runtime/value-serializer';

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
}
