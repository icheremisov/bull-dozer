import { DozerEngine, DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';
import { WorkflowJoinService } from '../support/workflow-join.service';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

@Workflow({ name: 'child-deep-workflow' })
export class ChildDeepWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(
    private readonly engine: DozerEngine,
    private readonly join: WorkflowJoinService,
    private readonly failureMemory: FailureMemoryService,
  ) {
    super();
  }

  @Step({ name: 'invoke-grandchild' })
  async invokeGrandchild(input: {
    id: string;
    value: number;
  }): Promise<number> {
    const delay = Math.floor(Math.random() * 20);
    await sleep(delay);
    const jobId = await this.engine.start('grandchild-workflow', input);
    return this.join.waitForResult<number>(jobId);
  }

  @Step({ name: 'child-fail-once' })
  failOnce(id: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `child-deep:fail:${id}`,
      1,
    );
    if (shouldFail) {
      return Promise.reject(new Error('child-deep-fail-once'));
    }

    return Promise.resolve();
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.invokeGrandchild(input).then(async (value) => {
      await this.failOnce(input.id);
      return value + 1;
    });
  }
}
