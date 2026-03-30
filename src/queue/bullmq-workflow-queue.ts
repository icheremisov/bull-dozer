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

  get priority(): number {
    return this.job.priority ?? 0;
  }

  get progress(): number | object {
    return this.job.progress ?? 0;
  }

  async updateData(data: WorkflowJobData<TInput>): Promise<void> {
    await this.job.updateData(data);
    this.dataSnapshot = data;
  }

  async log(row: string): Promise<number> {
    return (await this.job.log?.(row)) ?? 0;
  }

  async clearLogs(keepLast?: number): Promise<void> {
    await this.job.clearLogs?.(keepLast);
  }

  async changePriority(opts: {
    priority?: number;
    lifo?: boolean;
  }): Promise<void> {
    await this.job.changePriority?.(opts);
  }

  async updateProgress(progress: number | object): Promise<void> {
    await this.job.updateProgress?.(progress);
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

  async getJobLogs(jobId: string): Promise<{ logs: string[]; count: number }> {
    return (await this.queue.getJobLogs?.(jobId)) ?? { logs: [], count: 0 };
  }
}
