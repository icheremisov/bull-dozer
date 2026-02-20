import { DozerEngine, Step, Workflow } from 'dozer';
import { WorkflowJoinService } from '../support/workflow-join.service';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

@Workflow({ name: 'parent-deep-workflow' })
export class ParentDeepWorkflow {
  constructor(
    private readonly engine: DozerEngine,
    private readonly join: WorkflowJoinService,
  ) {}

  @Step({ name: 'invoke-child-deep' })
  async invokeChild(input: { id: string; value: number }): Promise<number> {
    const delay = Math.floor(Math.random() * 20);
    await sleep(delay);
    const jobId = await this.engine.start('child-deep-workflow', input);
    return this.join.waitForResult<number>(jobId);
  }

  @Step({ name: 'finalize' })
  finalize(value: number): Promise<{ value: number }> {
    return Promise.resolve({ value: value + 1 });
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const value = await this.invokeChild(input);
    return this.finalize(value);
  }
}
