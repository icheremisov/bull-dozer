import { DozerWorkflow, Step, Workflow } from 'dozer';
import { BranchSelectorService } from '../support/branch-selector.service';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'non-deterministic' })
export class NonDeterministicWorkflow extends DozerWorkflow<{
  id: string;
  value: number;
}> {
  constructor(
    private readonly selector: BranchSelectorService,
    private readonly failureMemory: FailureMemoryService,
  ) {
    super();
  }

  @Step({ name: 'left-branch' })
  left(value: number): Promise<number> {
    return Promise.resolve(value + 10);
  }

  @Step({ name: 'right-branch' })
  right(value: number): Promise<number> {
    return Promise.resolve(value + 20);
  }

  @Step({ name: 'must-fail-once' })
  failOnce(key: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(`nondet:${key}`, 1);
    if (shouldFail) {
      throw new Error('nondeterministic-first-fail');
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const branch = this.selector.pick(input.id);
    const value =
      branch === 'left'
        ? await this.left(input.value)
        : await this.right(input.value);

    await this.failOnce(input.id);
    return { value };
  }
}
