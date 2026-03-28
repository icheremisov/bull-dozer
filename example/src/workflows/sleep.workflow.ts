import { DozerWorkflow, Step, Workflow } from 'dozer';

@Workflow({ name: 'sleep-workflow' })
export class SleepWorkflow extends DozerWorkflow<{ id: string; durationMs: number; value: number }> {
  @Step({ name: 'prepare' })
  prepare(input: { id: string; value: number }): Promise<number> {
    return Promise.resolve(input.value);
  }

  @Step({ name: 'process' })
  process(value: number): Promise<number> {
    return Promise.resolve(value + 1);
  }

  async run(input: { id: string; durationMs: number; value: number }): Promise<{ value: number }> {
    const prepared = await this.prepare(input);
    await this.sleep(input.durationMs);
    const processed = await this.process(prepared);
    return { value: processed };
  }
}
