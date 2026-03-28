import { DozerEngine, DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';
import { WorkflowJoinService } from '../support/workflow-join.service';

interface ChildResult {
  childJobId: string;
  childValue: number;
}

@Workflow({ name: 'parent-workflow' })
export class ParentWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(
    private readonly engine: DozerEngine,
    private readonly join: WorkflowJoinService,
    private readonly failureMemory: FailureMemoryService,
  ) {
    super();
  }

  @Step({ name: 'invoke-child' })
  async invokeChild(input: {
    id: string;
    value: number;
  }): Promise<ChildResult> {
    const childJobId = await this.engine.start('child-workflow', input);
    const childValue = await this.join.waitForResult<number>(childJobId);

    return {
      childJobId,
      childValue,
    };
  }

  @Step({ name: 'finalize' })
  finalize(input: { id: string; childValue: number }): Promise<number> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `parent:finalize:${input.id}`,
      1,
    );
    if (shouldFail) {
      throw new Error('parent-finalize-fail-once');
    }

    return Promise.resolve(input.childValue + 1);
  }

  async run(input: {
    id: string;
    value: number;
  }): Promise<{ childJobId: string; value: number }> {
    const child = await this.invokeChild(input);
    const value = await this.finalize({
      id: input.id,
      childValue: child.childValue,
    });

    return {
      childJobId: child.childJobId,
      value,
    };
  }
}
