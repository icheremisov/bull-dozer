import { DOZER_JOB_INPUT_KEY, DOZER_JOB_STATE_KEY } from '../constants';

export const WORKFLOW_STATUS = {
  pending: 0,
  running: 1,
  failed: 2,
  completed: 3,
} as const;

export type WorkflowStatusCode =
  (typeof WORKFLOW_STATUS)[keyof typeof WORKFLOW_STATUS];

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

export type WorkflowJobOptions = Record<string, unknown>;

export interface WorkflowJob<TInput = unknown> {
  id: string;
  name: string;
  data: WorkflowJobData<TInput>;
  options?: WorkflowJobOptions;
  updateData(data: WorkflowJobData<TInput>): Promise<void>;
}

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
}

export interface BullMQQueueLike<TData> {
  add(
    name: string,
    data: TData,
    options?: WorkflowJobOptions,
  ): Promise<BullMQJobLike<unknown>>;
  getJob(jobId: string): Promise<BullMQJobLike<unknown> | null | undefined>;
}
