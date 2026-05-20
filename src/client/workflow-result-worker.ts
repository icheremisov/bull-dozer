import {
  Job,
  type ConnectionOptions,
  type WorkerOptions,
  Worker,
} from 'bullmq';
import { deserializeFromStorage } from '../runtime/value-serializer';
import type { WorkflowResultQueueJobData } from '../queue/workflow-queue';

export interface WorkflowResultMessage<TResult = unknown> {
  resultJobId: string;
  resultJobName: string;
  workflowJobId: string;
  workflowName: string;
  status: 'completed' | 'failed';
  result: TResult | null;
  error?: string;
}

export type WorkflowResultHandler<TResult = unknown, TReturn = unknown> = (
  message: WorkflowResultMessage<TResult>,
  job: Job<WorkflowResultQueueJobData<unknown>, TReturn, string>,
) => Promise<TReturn> | TReturn;

const assertResultQueuePayload = (
  value: unknown,
  jobIdForError: string,
): WorkflowResultQueueJobData<unknown> => {
  if (!value || typeof value !== 'object') {
    throw new Error(
      `Result queue job "${jobIdForError}" has invalid payload: expected object.`,
    );
  }

  const payload = value as Partial<WorkflowResultQueueJobData<unknown>>;
  if (typeof payload.jobId !== 'string') {
    throw new Error(
      `Result queue job "${jobIdForError}" has invalid payload: "jobId" must be string.`,
    );
  }
  if (typeof payload.workflowName !== 'string') {
    throw new Error(
      `Result queue job "${jobIdForError}" has invalid payload: "workflowName" must be string.`,
    );
  }
  if (!('result' in payload)) {
    throw new Error(
      `Result queue job "${jobIdForError}" has invalid payload: missing "result".`,
    );
  }

  return payload as WorkflowResultQueueJobData<unknown>;
};

export const decodeWorkflowResultJob = <TResult = unknown>(
  job: Job<WorkflowResultQueueJobData<unknown>, unknown, string>,
): WorkflowResultMessage<TResult> => {
  const resultJobId = String(job.id ?? '');
  const payload = assertResultQueuePayload(job.data, resultJobId || job.name);

  return {
    resultJobId,
    resultJobName: job.name,
    workflowJobId: payload.jobId,
    workflowName: payload.workflowName,
    status: payload.status ?? 'completed',
    result: deserializeFromStorage(payload.result) as TResult | null,
    error: payload.error,
  };
};

export const createWorkflowResultProcessor = <
  TResult = unknown,
  TReturn = unknown,
>(
  handler: WorkflowResultHandler<TResult, TReturn>,
): ((
  job: Job<WorkflowResultQueueJobData<unknown>, TReturn, string>,
) => Promise<TReturn>) => {
  return async (job) => {
    const message = decodeWorkflowResultJob<TResult>(job);
    return handler(message, job);
  };
};

export interface CreateWorkflowResultWorkerOptions<
  TResult = unknown,
  TReturn = unknown,
> {
  queueName: string;
  connection: ConnectionOptions;
  handler: WorkflowResultHandler<TResult, TReturn>;
  worker?: Omit<WorkerOptions, 'connection'>;
  prefix?: string;
}

export const createWorkflowResultWorker = <
  TResult = unknown,
  TReturn = unknown,
>(
  options: CreateWorkflowResultWorkerOptions<TResult, TReturn>,
): Worker<WorkflowResultQueueJobData<unknown>, TReturn, string> => {
  return new Worker<WorkflowResultQueueJobData<unknown>, TReturn, string>(
    options.queueName,
    createWorkflowResultProcessor(options.handler),
    {
      ...(options.worker ?? {}),
      connection: options.connection,
      ...(options.prefix ? { prefix: options.prefix } : {}),
    },
  );
};
