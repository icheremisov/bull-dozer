import { DozerError } from './dozer.error';

export class WorkflowNotRegisteredError extends DozerError {
  constructor(workflowName: string) {
    super(
      'WORKFLOW_NOT_REGISTERED',
      `Workflow "${workflowName}" is not registered in DI.`,
    );
  }
}
