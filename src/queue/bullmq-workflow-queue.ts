import {
  BullMQJobLike,
  BullMQQueueLike,
  WorkflowJob,
  WorkflowJobData,
  WorkflowJobOptions,
  WorkflowQueueDriver,
} from './workflow-queue';

class BullMQWorkflowJob<TInput = unknown> implements WorkflowJob<TInput> {
  private dataSnapshot: WorkflowJobData<TInput>;

  constructor(
    private readonly job: BullMQJobLike<WorkflowJobData<TInput>>,
    public readonly options?: WorkflowJobOptions,
  ) {
    this.dataSnapshot = job.data;
  }

  get id(): string {
    if (this.job.id === undefined || this.job.id === null) {
      throw new Error('BullMQ job id is undefined.');
    }

    return String(this.job.id);
  }

  get name(): string {
    return this.job.name;
  }

  get data(): WorkflowJobData<TInput> {
    return this.dataSnapshot;
  }

  async updateData(data: WorkflowJobData<TInput>): Promise<void> {
    await this.job.updateData(data);
    this.dataSnapshot = data;
  }
}

export class BullMQWorkflowQueue implements WorkflowQueueDriver {
  constructor(private readonly queue: BullMQQueueLike<unknown>) {}

  async add<TInput = unknown>(
    workflowName: string,
    data: WorkflowJobData<TInput>,
    options?: WorkflowJobOptions,
  ): Promise<WorkflowJob<TInput>> {
    const job = await this.queue.add(
      workflowName,
      data as WorkflowJobData<unknown>,
      options,
    );

    return new BullMQWorkflowJob<TInput>(
      job as BullMQJobLike<WorkflowJobData<TInput>>,
      options,
    );
  }

  async get<TInput = unknown>(
    jobId: string,
  ): Promise<WorkflowJob<TInput> | null> {
    const job = await this.queue.getJob(jobId);
    if (!job) {
      return null;
    }

    return new BullMQWorkflowJob<TInput>(
      job as BullMQJobLike<WorkflowJobData<TInput>>,
    );
  }

  async moveToDelayed(
    jobId: string,
    timestamp: number,
    token?: string,
  ): Promise<void> {
    const job = await this.queue.getJob(jobId);
    if (!job) return;
    const bullmqJob = job;
    await bullmqJob.moveToDelayed?.(timestamp, token);
  }

  async promoteDelayed(jobId: string): Promise<void> {
    const job = await this.queue.getJob(jobId);
    if (!job) return;
    const bullmqJob = job;
    await bullmqJob.promote?.();
  }
}
