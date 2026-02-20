import { DozerEngine, Step, Workflow } from 'dozer';
import { WorkflowJoinService } from '../support/workflow-join.service';

@Workflow({ name: 'parent-child-failing' })
export class ParentChildFailingWorkflow {
  constructor(
    private readonly engine: DozerEngine,
    private readonly join: WorkflowJoinService,
  ) {}

  @Step({ name: 'invoke-child' })
  async invokeChild(input: { id: string; value: number }): Promise<number> {
    const childJobId = await this.engine.start('child-failing', input);
    return this.join.waitForResult<number>(childJobId);
  }

  @Step({ name: 'finalize' })
  finalize(value: number): Promise<{ value: number }> {
    return Promise.resolve({ value: value + 1 });
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const childResult = await this.invokeChild(input);
    return this.finalize(childResult);
  }
}
