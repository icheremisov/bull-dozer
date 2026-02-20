import { Step, Workflow } from 'dozer';

@Workflow({ name: 'recursive' })
export class RecursiveWorkflow {
  @Step({ name: 'node' })
  async node(input: { value: number; depth: number }): Promise<number> {
    if (input.depth <= 0) {
      return this.leaf(input.value);
    }

    const nested = await this.node({
      value: input.value + 1,
      depth: input.depth - 1,
    });

    return nested + 1;
  }

  @Step({ name: 'leaf' })
  leaf(value: number): Promise<number> {
    return Promise.resolve(value);
  }

  run(input: { value: number; depth: number }): Promise<number> {
    return this.node(input);
  }
}
