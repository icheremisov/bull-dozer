import { Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

@Workflow({ name: 'batch-wait' })
export class BatchWaitWorkflow {
  constructor(private readonly failureMemory: FailureMemoryService) {}

  @Step({ name: 'unit' })
  async unit(input: { id: string; value: number }): Promise<number> {
    const delay = Math.floor(Math.random() * 30);
    await sleep(delay);
    this.failureMemory.markAndShouldFail(`batch-wait:unit:${input.id}`, 0);
    return input.value + 1;
  }

  @Step({ name: 'aggregate' })
  aggregate(values: number[]): Promise<{ sum: number; count: number }> {
    return Promise.resolve({
      sum: values.reduce((acc, value) => acc + value, 0),
      count: values.length,
    });
  }

  async run(input: {
    id: string;
    values: number[];
  }): Promise<{ sum: number; count: number }> {
    const chain = input.values.reduce<Promise<number[]>>((promise, value) => {
      return promise.then(async (accumulated) => {
        const next = await this.unit({
          id: input.id,
          value,
        });
        return [...accumulated, next];
      });
    }, Promise.resolve([]));

    const results = await chain;

    return this.aggregate(results);
  }
}
