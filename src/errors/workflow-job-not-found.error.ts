import { DozerError } from './dozer.error';

export class WorkflowJobNotFoundError extends DozerError {
  constructor(jobId: string) {
    super('WORKFLOW_JOB_NOT_FOUND', `Job "${jobId}" was not found.`);
  }
}
