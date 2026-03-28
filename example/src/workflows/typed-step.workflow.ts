import { DozerWorkflow, Step, Workflow } from 'dozer';

@Workflow({ name: 'typed-step' })
export class TypedStepWorkflow extends DozerWorkflow<{ value: number }> {
  @Step({ name: 'void-step' })
  touch(): Promise<void> {
    return Promise.resolve();
  }

  @Step({ name: 'undefined-step' })
  getUndefined(): Promise<undefined> {
    return Promise.resolve(undefined);
  }

  @Step({ name: 'number-step' })
  getNumber(value: number): Promise<number> {
    return Promise.resolve(value + 1);
  }

  async run(input: { value: number }): Promise<{ value: number }> {
    await this.touch();
    await this.getUndefined();
    const next = await this.getNumber(input.value);
    return { value: next };
  }
}
