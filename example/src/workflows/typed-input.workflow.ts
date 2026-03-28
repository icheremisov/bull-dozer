import { DozerWorkflow, Step, Workflow } from 'dozer';

export type TypedInput =
  | { kind: 'number'; value: number }
  | { kind: 'string'; value: string }
  | { kind: 'object'; value: { id: number; tag: string } }
  | { kind: 'array'; value: string[] }
  | { kind: 'null'; value: null };

@Workflow({ name: 'typed-input' })
export class TypedInputWorkflow extends DozerWorkflow<TypedInput> {
  @Step({ name: 'normalize' })
  normalize(input: TypedInput): Promise<string> {
    switch (input.kind) {
      case 'number':
        return Promise.resolve(String(input.value));
      case 'string':
        return Promise.resolve(input.value);
      case 'object':
        return Promise.resolve(`${input.value.id}:${input.value.tag}`);
      case 'array':
        return Promise.resolve(input.value.join(','));
      case 'null':
        return Promise.resolve('null');
    }
  }

  async run(input: TypedInput): Promise<{ normalized: string }> {
    const normalized = await this.normalize(input);
    return { normalized };
  }
}
