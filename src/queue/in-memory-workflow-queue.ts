import { WORKFLOW_QUEUE_NAME } from '../constants';
import {
  WorkflowJob,
  WorkflowJobData,
  WorkflowJobOptions,
  WorkflowQueueDriver,
} from './workflow-queue';

class InMemoryWorkflowJob<TInput = unknown> implements WorkflowJob<TInput> {
  constructor(
    public readonly id: string,
    public readonly name: string,
    public data: WorkflowJobData<TInput>,
    public readonly options?: WorkflowJobOptions,
  ) {}

  updateData(data: WorkflowJobData<TInput>): Promise<void> {
    this.data = data;
    return Promise.resolve();
  }
}

export class InMemoryWorkflowQueue implements WorkflowQueueDriver {
  private readonly jobs = new Map<string, WorkflowJob<unknown>>();
  private readonly delayedJobs = new Map<string, number>();
  private counter = 0;

  constructor(private readonly queueName = WORKFLOW_QUEUE_NAME) {}

  add<TInput = unknown>(
    workflowName: string,
    data: WorkflowJobData<TInput>,
    options?: WorkflowJobOptions,
  ): Promise<WorkflowJob<TInput>> {
    this.counter += 1;
    const jobId = `${this.queueName}:${this.counter}`;
    const job = new InMemoryWorkflowJob<TInput>(
      jobId,
      workflowName,
      data,
      options,
    );
    this.jobs.set(jobId, job as WorkflowJob<unknown>);
    return Promise.resolve(job);
  }

  get<TInput = unknown>(jobId: string): Promise<WorkflowJob<TInput> | null> {
    const job = this.jobs.get(jobId);
    return Promise.resolve((job as WorkflowJob<TInput> | undefined) ?? null);
  }

  moveToDelayed(jobId: string, timestamp: number): Promise<void> {
    if (this.jobs.has(jobId)) {
      this.delayedJobs.set(jobId, timestamp);
    }
    return Promise.resolve();
  }

  promoteDelayed(jobId: string): Promise<void> {
    this.delayedJobs.delete(jobId);
    return Promise.resolve();
  }

  /** Test helper — check if a job is currently in delayed state */
  isDelayed(jobId: string): boolean {
    return this.delayedJobs.has(jobId);
  }

  /** Test helper — return the timestamp passed to moveToDelayed, or undefined */
  getDelayedAt(jobId: string): number | undefined {
    return this.delayedJobs.get(jobId);
  }
}
