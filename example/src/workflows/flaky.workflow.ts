import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'flaky' })
export class FlakyWorkflow extends DozerWorkflow<{ key: string; threshold: number; failTimes?: number }> {
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'unstable', retry: { attempts: 5, backoffMs: 10 } })
  unstable(input: {
    key: string;
    threshold: number;
    failTimes?: number;
  }): Promise<{ success: true }> {
    const failTimes = input.failTimes ?? 0;
    if (failTimes > 0) {
      const shouldFail = this.failureMemory.markAndShouldFail(
        `flaky:${input.key}`,
        failTimes,
      );
      if (shouldFail) {
        throw new Error('deterministic-failure');
      }
    } else if (Math.random() < input.threshold) {
      throw new Error('random-failure');
    }

    return Promise.resolve({ success: true });
  }

  run(input: {
    key: string;
    threshold: number;
    failTimes?: number;
  }): Promise<{ success: true }> {
    return this.unstable(input);
  }
}
