import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

@Workflow({ name: 'long-running' })
export class LongRunningWorkflow extends DozerWorkflow<{ id: string; delayMs: number; failTimes: number }> {
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'prepare' })
  async prepare(input: {
    id: string;
    delayMs: number;
  }): Promise<{ id: string }> {
    await sleep(input.delayMs);
    return { id: input.id };
  }

  @Step({ name: 'work' })
  async work(input: {
    id: string;
    failTimes: number;
  }): Promise<{ id: string; worked: true }> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `long-running:${input.id}`,
      input.failTimes,
    );
    if (shouldFail) {
      throw new Error('long-running-failure');
    }

    await sleep(50);
    return { id: input.id, worked: true };
  }

  async run(input: {
    id: string;
    delayMs: number;
    failTimes: number;
  }): Promise<{ ok: true }> {
    const prepared = await this.prepare({
      id: input.id,
      delayMs: input.delayMs,
    });
    await this.work({ id: prepared.id, failTimes: input.failTimes });
    return { ok: true };
  }
}
