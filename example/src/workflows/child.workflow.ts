import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'child-workflow' })
export class ChildWorkflow extends DozerWorkflow<{
  id: string;
  value: number;
}> {
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'compute' })
  compute(input: { id: string; value: number }): Promise<number> {
    this.failureMemory.markAndShouldFail(`child:compute:${input.id}`, 0);
    return Promise.resolve(input.value * 2);
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.compute(input);
  }
}
