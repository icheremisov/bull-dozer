import {
  DozerWorkflow,
  NonRetryableError,
  NoStep,
  Step,
  Workflow,
  WorkflowWithFailureHandler,
} from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'on-failed' })
export class OnFailedWorkflow
  extends DozerWorkflow<{ id: string }>
  implements WorkflowWithFailureHandler<{ id: string }>
{
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'fail' })
  fail(id: string): Promise<void> {
    return Promise.reject(new NonRetryableError(`on-failed-error:${id}`));
  }

  async run(input: { id: string }): Promise<void> {
    await this.fail(input.id);
  }

  @NoStep()
  onFailed(_error: Error, input: { id: string }): void {
    this.failureMemory.markAndShouldFail(`on-failed:callback:${input.id}`, 0);
  }
}
