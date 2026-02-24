import { DozerError } from './dozer.error';

export class WorkflowCancelledError extends DozerError {
  constructor(jobId: string) {
    super('WORKFLOW_CANCELLED', `Job "${jobId}" was cancelled.`);
  }
}
