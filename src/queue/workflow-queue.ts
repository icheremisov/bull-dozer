import { DOZER_JOB_INPUT_KEY, DOZER_JOB_STATE_KEY } from '../constants';
import type {
  BackoffOptions,
  DeduplicationOptions,
  JobsOptions,
  KeepJobs,
  ParentOptions,
  RepeatOptions,
} from 'bullmq';

export const WORKFLOW_STATUS = {
  pending: 0,
  running: 1,
  failed: 2,
  completed: 3,
  cancelled: 4,
  completing: 5,
} as const;

export type WorkflowStatusCode =
  (typeof WORKFLOW_STATUS)[keyof typeof WORKFLOW_STATUS];
export type WorkflowStatusName = keyof typeof WORKFLOW_STATUS;

export interface CompactWorkflowState {
  s: WorkflowStatusCode;
  c: Record<string, unknown>;
  a?: Record<string, number>;
  u?: Record<string, 1>;
  t: string[];
  r?: unknown;
  e?: string;
}

export interface WorkflowJobData<TInput = unknown> {
  [DOZER_JOB_INPUT_KEY]: TInput;
  [DOZER_JOB_STATE_KEY]?: CompactWorkflowState;
}

export type WorkflowKeepJobsOptions = KeepJobs;
export type WorkflowJobBackoffOptions = BackoffOptions;
export type WorkflowJobParentOptions = ParentOptions;
export type WorkflowJobDeduplicationOptions = DeduplicationOptions;
export type WorkflowJobRepeatOptions = RepeatOptions;
export type WorkflowJobOptions = JobsOptions;

export interface WorkflowJob<TInput = unknown> {
  id: string;
  name: string;
  data: WorkflowJobData<TInput>;
  options?: WorkflowJobOptions;
  updateData(data: WorkflowJobData<TInput>): Promise<void>;
}

export interface WorkflowJobInfo<TResult = unknown> {
  id: string;
  name: string;
  status: WorkflowStatusCode;
  statusName: WorkflowStatusName;
  result?: TResult;
  error?: string;
}

export interface WorkflowResultQueueJobData<TResult = unknown> {
  jobId: string;
  workflowName: string;
  result: TResult;
}

export interface WorkflowResultQueueJobInfo<TResult = unknown> {
  id: string;
  name: string;
  jobId: string;
  workflowName: string;
  result: TResult;
}

export const toWorkflowResultQueueJobId = (workflowJobId: string): string => {
  if (/^\d+$/.test(workflowJobId)) {
    return `#${workflowJobId}`;
  }

  return workflowJobId;
};

export interface WorkflowQueueDriver {
  add<TInput = unknown>(
    workflowName: string,
    data: WorkflowJobData<TInput>,
    options?: WorkflowJobOptions,
  ): Promise<WorkflowJob<TInput>>;
  get<TInput = unknown>(jobId: string): Promise<WorkflowJob<TInput> | null>;
}

export interface BullMQJobLike<TData> {
  id?: string | number;
  name: string;
  data: TData;
  updateData(data: TData): Promise<void>;
  getState?(): Promise<string>;
}

export interface BullMQQueueLike<TData> {
  add(
    name: string,
    data: TData,
    options?: WorkflowJobOptions,
  ): Promise<BullMQJobLike<unknown>>;
  getJob(jobId: string): Promise<BullMQJobLike<unknown> | null | undefined>;
}
