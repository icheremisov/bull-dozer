export { DozerModule } from './dozer.module';
export type {
  DozerDefaultsOptions,
  DozerModuleAsyncOptions,
  DozerModuleOptions,
} from './dozer.module';

export { DozerEngine } from './engine/dozer-engine';
export { DozerClient } from './client/dozer-client';
export {
  createWorkflowResultWorker,
  createWorkflowResultProcessor,
  decodeWorkflowResultJob,
} from './client/workflow-result-worker';
export type {
  CreateWorkflowResultWorkerOptions,
  WorkflowResultHandler,
  WorkflowResultMessage,
} from './client/workflow-result-worker';

export { Workflow } from './decorators/workflow.decorator';
export type {
  WorkflowExecutionOptions,
  WorkflowResultQueueOptions,
  WorkflowOptions,
} from './decorators/workflow.decorator';
export { Step } from './decorators/step.decorator';
export type { RetryOptions, StepOptions } from './decorators/step.decorator';
export type { WorkflowWithFailureHandler } from './workflow/workflow-with-failure-handler';

export {
  DOZER_JOB_INPUT_KEY,
  DOZER_JOB_STATE_KEY,
  WORKFLOW_OPTIONS_METADATA,
  STEP_OPTIONS_METADATA,
  WORKFLOW_QUEUE_NAME,
} from './constants';

export { WORKFLOW_STATUS } from './queue/workflow-queue';
export { toWorkflowResultQueueJobId } from './queue/workflow-queue';
export type {
  BullMQQueueLike,
  BullMQJobLike,
  WorkflowStatusCode,
  WorkflowStatusName,
  WorkflowKeepJobsOptions,
  WorkflowJobBackoffOptions,
  WorkflowJobParentOptions,
  WorkflowJobDeduplicationOptions,
  WorkflowJobRepeatOptions,
  WorkflowJobOptions,
  CompactWorkflowState,
  WorkflowJobData,
  WorkflowJob,
  WorkflowJobInfo,
  WorkflowResultQueueJobData,
  WorkflowResultQueueJobInfo,
  WorkflowQueueDriver,
} from './queue/workflow-queue';
export type { WaitForWorkflowResultOptions } from './client/dozer-client';
export { BullMQWorkflowQueue } from './queue/bullmq-workflow-queue';
export { InMemoryWorkflowQueue } from './queue/in-memory-workflow-queue';

export { DozerError } from './errors/dozer.error';
export { NonDeterminismError } from './errors/non-determinism.error';
export { NonRetryableError } from './errors/non-retryable.error';
export { SerializationError } from './errors/serialization.error';
export { StepReplayConflictError } from './errors/step-replay-conflict.error';
export { TimeoutError } from './errors/timeout.error';
export { WorkflowJobNotFoundError } from './errors/workflow-job-not-found.error';
export { WorkflowCancelledError } from './errors/workflow-cancelled.error';
export { WorkflowNotRegisteredError } from './errors/workflow-not-registered.error';
