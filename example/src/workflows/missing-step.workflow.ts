import { DozerWorkflow, NoStep, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'missing-step' })
export class MissingStepWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  @NoStep()
  plainCompute(input: { id: string; value: number }): number {
    this.failureMemory.markAndShouldFail(`missing-step:plain:${input.id}`, 0);
    return input.value + 1;
  }

  @Step({ name: 'fail-once' })
  failOnce(id: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `missing-step:fail:${id}`,
      1,
    );
    if (shouldFail) {
      throw new Error('missing-step-fail-once');
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const value = this.plainCompute(input);
    await this.failOnce(input.id);
    return { value };
  }
}
