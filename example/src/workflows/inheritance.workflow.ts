import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

abstract class InheritanceBase extends DozerWorkflow<unknown> {
  constructor(protected readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'base-step' })
  baseStep(input: { id: string; value: number }): Promise<number> {
    this.failureMemory.markAndShouldFail(`inheritance:base:${input.id}`, 0);
    return Promise.resolve(input.value + 1);
  }

  @Step({ name: 'poly-step' })
  polyStep(input: { id: string; value: number }): Promise<number> {
    this.failureMemory.markAndShouldFail(
      `inheritance:poly-base:${input.id}`,
      0,
    );
    return Promise.resolve(input.value * 2);
  }
}

@Workflow({ name: 'inheritance' })
export class InheritanceWorkflow extends InheritanceBase {
  constructor(failureMemory: FailureMemoryService) {
    super(failureMemory);
  }

  @Step({ name: 'poly-step' })
  polyStep(input: { id: string; value: number }): Promise<number> {
    this.failureMemory.markAndShouldFail(
      `inheritance:poly-override:${input.id}`,
      0,
    );
    return Promise.resolve(input.value * 3);
  }

  @Step({ name: 'fail-once' })
  failOnce(id: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `inheritance:fail:${id}`,
      1,
    );
    if (shouldFail) {
      return Promise.reject(new Error('inheritance-fail-once'));
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const base = await this.baseStep(input);
    const poly = await this.polyStep({ id: input.id, value: base });
    await this.failOnce(input.id);
    return { value: poly };
  }
}
