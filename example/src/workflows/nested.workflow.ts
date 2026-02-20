import { Step, Workflow } from 'dozer';

@Workflow({ name: 'nested' })
export class NestedWorkflow {
  @Step({ name: 'outer' })
  async outer(input: number): Promise<number> {
    const a = await this.innerA(input);
    const b = await this.innerB(a);
    return b;
  }

  @Step({ name: 'inner-a' })
  innerA(input: number): Promise<number> {
    return Promise.resolve(input + 1);
  }

  @Step({ name: 'inner-b' })
  innerB(input: number): Promise<number> {
    return Promise.resolve(input + 1);
  }

  run(input: { value: number }): Promise<number> {
    return this.outer(input.value);
  }
}
