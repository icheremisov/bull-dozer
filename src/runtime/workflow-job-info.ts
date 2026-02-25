import { DOZER_JOB_STATE_KEY } from '../constants';
import type {
  CompactWorkflowState,
  WorkflowJob,
  WorkflowJobInfo,
  WorkflowStatusCode,
  WorkflowStatusName,
} from '../queue/workflow-queue';
import { WORKFLOW_STATUS } from '../queue/workflow-queue';
import { deserializeFromStorage } from './value-serializer';

const STATUS_NAME_BY_CODE: Record<WorkflowStatusCode, WorkflowStatusName> = {
  [WORKFLOW_STATUS.pending]: 'pending',
  [WORKFLOW_STATUS.running]: 'running',
  [WORKFLOW_STATUS.failed]: 'failed',
  [WORKFLOW_STATUS.completed]: 'completed',
  [WORKFLOW_STATUS.cancelled]: 'cancelled',
  [WORKFLOW_STATUS.completing]: 'completing',
};

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === 'object' && value !== null;
};

const isWorkflowStatusCode = (value: unknown): value is WorkflowStatusCode => {
  return Object.values(WORKFLOW_STATUS).includes(value as WorkflowStatusCode);
};

const isCompactWorkflowState = (
  value: unknown,
): value is CompactWorkflowState => {
  if (!isRecord(value)) {
    return false;
  }

  return (
    isWorkflowStatusCode(value.s) &&
    isRecord(value.c) &&
    (value.a === undefined || isRecord(value.a)) &&
    (value.u === undefined || isRecord(value.u)) &&
    Array.isArray(value.t)
  );
};

const getJobState = (job: WorkflowJob<unknown>): CompactWorkflowState => {
  const persistedState = job.data[DOZER_JOB_STATE_KEY];
  if (isCompactWorkflowState(persistedState)) {
    return persistedState;
  }

  return {
    s: WORKFLOW_STATUS.pending,
    c: {},
    a: {},
    t: [],
  };
};

export const getWorkflowJobInfo = <TResult = unknown>(
  job: WorkflowJob<unknown>,
): WorkflowJobInfo<TResult> => {
  const state = getJobState(job);
  const status = state.s;

  return {
    id: job.id,
    name: job.name,
    status,
    statusName: STATUS_NAME_BY_CODE[status],
    result:
      (status === WORKFLOW_STATUS.completed ||
        status === WORKFLOW_STATUS.completing) &&
      'r' in state
        ? (deserializeFromStorage(state.r) as TResult)
        : undefined,
    error:
      (status === WORKFLOW_STATUS.failed ||
        status === WORKFLOW_STATUS.cancelled) &&
      typeof state.e === 'string'
        ? state.e
        : undefined,
  };
};
