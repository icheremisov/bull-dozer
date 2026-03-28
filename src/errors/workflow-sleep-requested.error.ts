import { DozerError } from './dozer.error';

export class WorkflowSleepRequestedError extends DozerError {
  constructor(public readonly wakeUpAt: number) {
    super(
      'WORKFLOW_SLEEP_REQUESTED',
      `Workflow sleep requested until ${new Date(wakeUpAt).toISOString()}`,
    );
    this.name = 'WorkflowSleepRequestedError';
  }
}
