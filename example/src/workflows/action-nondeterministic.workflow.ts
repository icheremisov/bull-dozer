import { Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'action-nondeterministic' })
export class ActionNondeterministicWorkflow {
  constructor(private readonly failureMemory: FailureMemoryService) {}

  @Step({ name: 'randomize' })
  randomize(id: string): Promise<number> {
    this.failureMemory.markAndShouldFail(`action-nondet:random:${id}`, 0);
    return Promise.resolve(Math.random());
  }

  @Step({ name: 'fail-once' })
  failOnce(id: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `action-nondet:fail:${id}`,
      1,
    );
    if (shouldFail) {
      throw new Error('action-nondet-fail-once');
    }

    return Promise.resolve();
  }

  async run(input: { id: string }): Promise<{ value: number }> {
    const value = await this.randomize(input.id);
    await this.failOnce(input.id);
    return { value };
  }
}
