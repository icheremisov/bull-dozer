import { DozerError } from './dozer.error';

export class WorkflowSignalWaitRequestedError extends DozerError {
  constructor(
    public readonly signalName: string,
    public readonly expiresAt?: number,
  ) {
    super(
      'WORKFLOW_SIGNAL_WAIT_REQUESTED',
      `Workflow waiting for signal "${signalName}"`,
    );
    this.name = 'WorkflowSignalWaitRequestedError';
  }
}
