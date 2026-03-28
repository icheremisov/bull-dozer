import { Injectable } from '@nestjs/common';
import {
  BullMQQueueLike,
  Step,
  Workflow,
  WorkflowJobOptions,
  WorkflowResultQueueJobData,
} from '../index';

export const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

@Injectable()
export class FailOnceService {
  private readonly map = new Map<string, number>();

  shouldFail(key: string, failTimes = 1): boolean {
    const current = this.map.get(key) ?? 0;
    this.map.set(key, current + 1);
    return current < failTimes;
  }

  calls(key: string): number {
    return this.map.get(key) ?? 0;
  }

  reset(): void {
    this.map.clear();
  }
}

@Workflow({ name: 'retry-workflow' })
export class RetryWorkflow {
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

export class CapturingResultQueue implements BullMQQueueLike<
  WorkflowResultQueueJobData<unknown>
> {
  readonly added: Array<{
    name: string;
    data: WorkflowResultQueueJobData<unknown>;
    options?: WorkflowJobOptions;
  }> = [];

  add(
    name: string,
    data: WorkflowResultQueueJobData<unknown>,
    options?: WorkflowJobOptions,
  ) {
    this.added.push({ name, data, options });
    return Promise.resolve({
      id: this.added.length,
      name,
      data,
      updateData: (
        nextData: WorkflowResultQueueJobData<unknown>,
      ): Promise<void> => {
        const index = this.added.length - 1;
        this.added[index] = { name, data: nextData, options };
        return Promise.resolve();
      },
    });
  }

  getJob(_jobId?: string): Promise<{
    id: string | number;
    name: string;
    data: WorkflowResultQueueJobData<unknown>;
    updateData: (nextData: WorkflowResultQueueJobData<unknown>) => Promise<void>;
  } | null> {
    return Promise.resolve(null);
  }
}

export class FailOnceResultQueue extends CapturingResultQueue {
  private failed = false;

  override add(
    name: string,
    data: WorkflowResultQueueJobData<unknown>,
    options?: WorkflowJobOptions,
  ) {
    if (!this.failed) {
      this.failed = true;
      return Promise.reject(new Error('result-queue-temporary-failure'));
    }

    return super.add(name, data, options);
  }
}

export class DuplicateJobIdResultQueue extends CapturingResultQueue {
  private readonly jobsById = new Map<
    string,
    WorkflowResultQueueJobData<unknown>
  >();

  override add(
    name: string,
    data: WorkflowResultQueueJobData<unknown>,
    options?: WorkflowJobOptions,
  ) {
    const jobId = options?.jobId;
    const normalizedJobId =
      jobId === undefined || jobId === null ? undefined : String(jobId);

    if (normalizedJobId && this.jobsById.has(normalizedJobId)) {
      return Promise.reject(new Error(`Job ${normalizedJobId} already exists`));
    }

    if (normalizedJobId) {
      this.jobsById.set(normalizedJobId, data);
    }

    return super.add(name, data, options);
  }

  override getJob(jobId?: string) {
    if (!jobId) {
      return Promise.resolve(null);
    }

    const data = this.jobsById.get(String(jobId));
    if (!data) {
      return Promise.resolve(null);
    }

    return Promise.resolve({
      id: String(jobId),
      name: 'workflow-result',
      data,
      updateData: (
        nextData: WorkflowResultQueueJobData<unknown>,
      ): Promise<void> => {
        this.jobsById.set(String(jobId), nextData);
        return Promise.resolve();
      },
    });
  }
}
