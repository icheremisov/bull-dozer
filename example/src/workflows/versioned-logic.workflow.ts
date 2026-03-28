import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';
import { ScenarioControlsService } from '../support/scenario-controls.service';

@Workflow({ name: 'versioned-logic' })
export class VersionedLogicWorkflow extends DozerWorkflow<{ id: string; value: number }> {
  constructor(
    private readonly controls: ScenarioControlsService,
    private readonly failureMemory: FailureMemoryService,
  ) {
    super();
  }

  @Step({ name: 'logic-v1' })
  logicV1(value: number): Promise<number> {
    return Promise.resolve(value + 10);
  }

  @Step({ name: 'logic-v2' })
  logicV2(value: number): Promise<number> {
    return Promise.resolve(value + 20);
  }

  @Step({ name: 'fail-once' })
  failOnce(id: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `versioned:fail:${id}`,
      1,
    );
    if (shouldFail) {
      throw new Error('versioned-fail-once');
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const version = this.controls.getVersion(input.id);
    const value =
      version === 'v2'
        ? await this.logicV2(input.value)
        : await this.logicV1(input.value);

    await this.failOnce(input.id);
    return { value };
  }
}
