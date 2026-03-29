import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'duplicate-step-name' })
export class DuplicateStepNameWorkflow extends DozerWorkflow<{
  id: string;
  value: number;
}> {
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'same-name' })
  first(input: { id: string; value: number }): Promise<number> {
    this.failureMemory.markAndShouldFail(`duplicate-step:first:${input.id}`, 0);
    return Promise.resolve(input.value + 1);
  }

  @Step({ name: 'same-name' })
  second(input: { id: string; value: number }): Promise<number> {
    this.failureMemory.markAndShouldFail(
      `duplicate-step:second:${input.id}`,
      0,
    );
    const shouldFail = this.failureMemory.markAndShouldFail(
      `duplicate-step:fail:${input.id}`,
      1,
    );
    if (shouldFail) {
      return Promise.reject(new Error('duplicate-step-fail-once'));
    }

    return Promise.resolve(input.value + 2);
  }

  async run(input: { id: string; value: number }): Promise<number> {
    const first = await this.first(input);
    return this.second({ id: input.id, value: first });
  }
}
