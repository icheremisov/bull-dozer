import { NonRetryableError, Step, Workflow, WorkflowWithFailureHandler } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'on-failed' })
export class OnFailedWorkflow
  implements WorkflowWithFailureHandler<{ id: string }>
{
  constructor(private readonly failureMemory: FailureMemoryService) {}

  @Step({ name: 'fail' })
  fail(id: string): Promise<void> {
    return Promise.reject(new NonRetryableError(`on-failed-error:${id}`));
  }

  async run(input: { id: string }): Promise<void> {
    await this.fail(input.id);
  }

  async onFailed(
    _error: Error,
    input: { id: string },
    _jobId: string,
  ): Promise<void> {
    this.failureMemory.markAndShouldFail(`on-failed:callback:${input.id}`, 0);
  }
}
