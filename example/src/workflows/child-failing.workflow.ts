import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'child-failing' })
export class ChildFailingWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'prepare' })
  prepare(input: { id: string; value: number }): Promise<number> {
    this.failureMemory.markAndShouldFail(
      `child-failing:prepare:${input.id}`,
      0,
    );
    return Promise.resolve(input.value + 1);
  }

  @Step({ name: 'process' })
  process(input: { id: string; prepared: number }): Promise<number> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `child-failing:process:${input.id}`,
      1,
    );
    if (shouldFail) {
      return Promise.reject(new Error('child-failing-process-fail-once'));
    }

    return Promise.resolve(input.prepared * 2);
  }

  async run(input: { id: string; value: number }): Promise<number> {
    const prepared = await this.prepare(input);
    return this.process({
      id: input.id,
      prepared,
    });
  }
}
