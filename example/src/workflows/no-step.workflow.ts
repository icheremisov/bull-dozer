import { DozerWorkflow, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'no-step' })
export class NoStepWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  run(input: { id: string; value: number }): Promise<{ value: number }> {
    this.failureMemory.markAndShouldFail(`no-step:run:${input.id}`, 0);

    const shouldFail = this.failureMemory.markAndShouldFail(
      `no-step:fail:${input.id}`,
      1,
    );
    if (shouldFail) {
      return Promise.reject(new Error('no-step-fail-once'));
    }

    return Promise.resolve({ value: input.value + 1 });
  }
}
