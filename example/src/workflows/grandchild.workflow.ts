import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

@Workflow({ name: 'grandchild-workflow' })
export class GrandchildWorkflow extends DozerWorkflow<{
  id: string;
  value: number;
}> {
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'grandchild-work' })
  async work(input: { id: string; value: number }): Promise<number> {
    const delay = Math.floor(Math.random() * 30);
    await sleep(delay);
    this.failureMemory.markAndShouldFail(`grandchild:work:${input.id}`, 0);
    return input.value + 1;
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.work(input);
  }
}
