import { Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

class PlainOverrideBaseWorkflow {
  constructor(protected readonly failureMemory: FailureMemoryService) {}

  dispatch(input: { id: string; value: number }): number {
    return this.target(input);
  }

  @Step({ name: 'target-base' })
  target(input: { id: string; value: number }): number {
    this.failureMemory.markAndShouldFail(
      `inheritance-plain:target-base:${input.id}`,
      0,
    );
    return input.value + 1;
  }
}

@Workflow({ name: 'inheritance-plain-override' })
export class InheritancePlainOverrideWorkflow extends PlainOverrideBaseWorkflow {
  constructor(failureMemory: FailureMemoryService) {
    super(failureMemory);
  }

  // intentionally no @Step
  target(input: { id: string; value: number }): number {
    this.failureMemory.markAndShouldFail(
      `inheritance-plain:target-derived:${input.id}`,
      0,
    );
    return input.value + 7;
  }

  @Step({ name: 'fail-once' })
  failOnce(id: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `inheritance-plain:fail:${id}`,
      1,
    );
    if (shouldFail) {
      return Promise.reject(new Error('inheritance-plain-fail-once'));
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const value = this.dispatch(input);
    await this.failOnce(input.id);
    return { value };
  }
}
