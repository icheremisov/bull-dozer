import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'replay' })
export class ReplayWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'first' })
  first(input: {
    id: string;
    value: number;
  }): Promise<{ id: string; value: number }> {
    this.failureMemory.markAndShouldFail(`replay-first:${input.id}`, 0);
    return Promise.resolve(input);
  }

  @Step({ name: 'second' })
  second(input: {
    id: string;
    value: number;
  }): Promise<{ id: string; value: number }> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `replay:${input.id}`,
      1,
    );
    if (shouldFail) {
      throw new Error('replay-failure-once');
    }

    return Promise.resolve({ ...input, value: input.value + 1 });
  }

  async run(input: {
    id: string;
    value: number;
  }): Promise<{ id: string; value: number }> {
    const first = await this.first(input);
    return this.second(first);
  }
}
