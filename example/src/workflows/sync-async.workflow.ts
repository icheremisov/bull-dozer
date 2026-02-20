import { Step, Workflow } from 'dozer';

@Workflow({ name: 'sync-async' })
export class SyncAsyncWorkflow {
  @Step({ name: 'sync-step' })
  syncStep(value: number): number {
    return value + 1;
  }

  @Step({ name: 'async-step' })
  asyncStep(value: number): Promise<number> {
    return Promise.resolve(value * 2);
  }

  async run(input: { value: number }): Promise<number> {
    const sync = await Promise.resolve(this.syncStep(input.value));
    return this.asyncStep(sync);
  }
}
