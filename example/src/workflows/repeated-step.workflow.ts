import { DozerWorkflow, Step, Workflow } from 'dozer';

@Workflow({ name: 'repeated-step' })
export class RepeatedStepWorkflow extends DozerWorkflow<{ value: number }> {
  @Step({ name: 'increment' })
  increment(value: number): Promise<number> {
    return Promise.resolve(value + 1);
  }

  async run(input: { value: number }): Promise<number> {
    const first = await this.increment(input.value);
    const second = await this.increment(first);
    return second;
  }
}
