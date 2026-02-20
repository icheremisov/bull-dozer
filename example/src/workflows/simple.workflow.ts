import { Step, Workflow } from 'dozer';

type Payload = Record<string, unknown>;

@Workflow({ name: 'simple' })
export class SimpleWorkflow {
  @Step({ name: 'validate' })
  validate(input: Payload): Promise<Payload> {
    return Promise.resolve({ ...input, validated: true });
  }

  @Step({ name: 'process' })
  process(input: Payload): Promise<Payload> {
    return Promise.resolve({ ...input, processed: true });
  }

  @Step({ name: 'store' })
  store(input: Payload): Promise<Payload> {
    return Promise.resolve({ ...input, stored: true });
  }

  async run(input: Payload): Promise<{ ok: true; payload: Payload }> {
    const validated = await this.validate(input);
    const processed = await this.process(validated);
    const stored = await this.store(processed);
    return { ok: true, payload: stored };
  }
}
