import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

abstract class DispatchBaseWorkflow extends DozerWorkflow<unknown> {
  constructor(protected readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'dispatch' })
  dispatch(input: { id: string; value: number }): Promise<number> {
    return this.target(input);
  }

  @Step({ name: 'target-base' })
  target(input: { id: string; value: number }): Promise<number> {
    this.failureMemory.markAndShouldFail(
      `inheritance-dispatch:target-base:${input.id}`,
      0,
    );
    return Promise.resolve(input.value + 1);
  }
}

@Workflow({ name: 'inheritance-dispatch' })
export class InheritanceDispatchWorkflow extends DispatchBaseWorkflow {
  constructor(failureMemory: FailureMemoryService) {
    super(failureMemory);
  }

  @Step({ name: 'target-derived' })
  target(input: { id: string; value: number }): Promise<number> {
    this.failureMemory.markAndShouldFail(
      `inheritance-dispatch:target-derived:${input.id}`,
      0,
    );
    return Promise.resolve(input.value + 5);
  }

  @Step({ name: 'fail-once' })
  failOnce(id: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `inheritance-dispatch:fail:${id}`,
      1,
    );
    if (shouldFail) {
      return Promise.reject(new Error('inheritance-dispatch-fail-once'));
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const value = await this.dispatch(input);
    await this.failOnce(input.id);
    return { value };
  }
}
