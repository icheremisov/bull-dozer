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
  WorkflowJobData,
  WorkflowJobOptions,
  WorkflowQueueDriver,
} from '../queue/workflow-queue';
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
}
