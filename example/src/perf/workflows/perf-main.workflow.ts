import { DozerEngine, DozerWorkflow, Step, Workflow } from 'dozer';
import { WorkflowJoinService } from '../../support/workflow-join.service';
import { PerfFailureService } from '../services/perf-failure.service';
import { PerfChildInput } from './perf-child.workflow';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

export interface PerfMainInput {
  key: string;
  payload: string;
  stepCount: number;
  nestedChildren: number;
  timerMs: number;
  failureRate: number;
}

@Workflow({
  name: 'perf-main-workflow',
  execution: {
    stepRetry: {
      attempts: 3,
      delayMs: 1,
      strategy: 'constant',
    },
  },
})
export class PerfMainWorkflow extends DozerWorkflow<PerfMainInput> {
  constructor(
    private readonly engine: DozerEngine,
    private readonly join: WorkflowJoinService,
    private readonly failure: PerfFailureService,
  ) {
    super();
  }

  @Step({ name: 'step-work' })
  async stepWork(input: {
    key: string;
    stepIndex: number;
    payload: string;
    timerMs: number;
    failureRate: number;
  }): Promise<string> {
    if (input.timerMs > 0) {
      await sleep(input.timerMs);
    }

    if (
      this.failure.shouldFailOnce(
        `${input.key}:step:${input.stepIndex}`,
        input.failureRate,
      )
    ) {
      throw new Error('perf-main-transient-failure');
    }

    return input.payload;
  }

  @Step({ name: 'invoke-children' })
  async invokeChildren(input: PerfMainInput): Promise<number> {
    if (input.nestedChildren <= 0) {
      return 0;
    }

    const childJobs = await Promise.all(
      Array.from({ length: input.nestedChildren }, async (_, childIndex) => {
        const childInput: PerfChildInput = {
          key: `${input.key}:child:${childIndex}`,
          payload: input.payload,
          timerMs: input.timerMs,
          failureRate: input.failureRate,
        };
        const childJobId = await this.engine.start(
          'perf-child-workflow',
          childInput,
        );
        return childJobId;
      }),
    );

    const childResults = await Promise.all(
      childJobs.map((jobId) =>
        this.join.waitForResult<{ payloadBytes: number }>(jobId, 60000),
      ),
    );

    return childResults.reduce((sum, current) => sum + current.payloadBytes, 0);
  }

  async run(
    input: PerfMainInput,
  ): Promise<{ bytes: number; nestedBytes: number }> {
    let payload = input.payload;
    for (let stepIndex = 0; stepIndex < input.stepCount; stepIndex += 1) {
      payload = await this.stepWork({
        key: input.key,
        stepIndex,
        payload,
        timerMs: input.timerMs,
        failureRate: input.failureRate,
      });
    }

    const nestedBytes = await this.invokeChildren(input);
    return {
      bytes: payload.length,
      nestedBytes,
    };
  }
}
