import { DozerWorkflow, Step, Workflow } from 'dozer';
import { PerfFailureService } from '../services/perf-failure.service';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

export interface PerfChildInput {
  key: string;
  payload: string;
  timerMs: number;
  failureRate: number;
}

@Workflow({
  name: 'perf-child-workflow',
  execution: {
    stepRetry: {
      attempts: 3,
      delayMs: 1,
      strategy: 'constant',
    },
  },
})
export class PerfChildWorkflow extends DozerWorkflow<PerfChildInput> {
  constructor(private readonly failure: PerfFailureService) {
    super();
  }

  @Step({ name: 'child-process' })
  async process(input: PerfChildInput): Promise<{ payloadBytes: number }> {
    if (input.timerMs > 0) {
      await sleep(input.timerMs);
    }

    if (
      this.failure.shouldFailOnce(
        `${input.key}:child:process`,
        input.failureRate,
      )
    ) {
      throw new Error('perf-child-transient-failure');
    }

    return {
      payloadBytes: input.payload.length,
    };
  }

  run(input: PerfChildInput): Promise<{ payloadBytes: number }> {
    return this.process(input);
  }
}
