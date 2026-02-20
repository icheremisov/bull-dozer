import { Inject, Injectable } from '@nestjs/common';
import { DOZER_JOB_STATE_KEY, WORKFLOW_STATUS, WorkflowJobData } from 'dozer';
import { Queue } from 'bullmq';
import { EXAMPLE_WORKFLOW_QUEUE } from '../infra/tokens';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

@Injectable()
export class WorkflowJoinService {
  constructor(
    @Inject(EXAMPLE_WORKFLOW_QUEUE)
    private readonly queue: Queue<WorkflowJobData<unknown>>,
  ) {}

  async waitForResult<TResult = unknown>(
    jobId: string,
    timeoutMs = 15000,
  ): Promise<TResult> {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      const job = await this.queue.getJob(jobId);
      if (job) {
        const state = job.data[DOZER_JOB_STATE_KEY];
        if (state?.s === WORKFLOW_STATUS.completed) {
          return state.r as TResult;
        }
        if (state?.s === WORKFLOW_STATUS.failed) {
          throw new Error(String(state.e ?? `workflow ${jobId} failed`));
        }
      }

      await sleep(25);
    }

    throw new Error(`Timed out waiting nested workflow job ${jobId}`);
  }
}
