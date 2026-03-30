import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

export type ParallelStressInput = {
  id: string;
  seed: number;
  coeffA: number;
  coeffB: number;
  steps: number;
  failAtStep?: number;
  sleepAfterStep?: number;
  sleepMs?: number;
};

@Workflow({ name: 'parallel-stress' })
export class ParallelStressWorkflow extends DozerWorkflow<ParallelStressInput> {
  constructor(private readonly failureMemory: FailureMemoryService) {
    super();
  }

  @Step({ name: 'init' })
  init(seed: number): Promise<number> {
    return Promise.resolve(seed);
  }

  @Step({ name: 'compute', retry: { attempts: 3, backoffMs: 20 } })
  computeStep(
    value: number,
    coeffA: number,
    coeffB: number,
    jobId: string,
    stepIdx: number,
    failable: boolean,
  ): Promise<number> {
    if (
      failable &&
      this.failureMemory.markAndShouldFail(`${jobId}:s${stepIdx}`, 1)
    ) {
      throw new Error(`Induced failure at step ${stepIdx}`);
    }
    const MOD = 1_000_000_007;
    return Promise.resolve((value * coeffA + coeffB) % MOD);
  }

  @Step({ name: 'schedule-sleep' })
  scheduleSleep(sleepMs: number): Promise<number> {
    return Promise.resolve(Date.now() + sleepMs);
  }

  async run(
    input: ParallelStressInput,
  ): Promise<{ id: string; value: number; steps: number }> {
    const {
      id,
      seed,
      coeffA,
      coeffB,
      steps,
      failAtStep,
      sleepAfterStep,
      sleepMs,
    } = input;

    let value = await this.init(seed);
    await this.log(
      `start id=${id} seed=${seed} coeffA=${coeffA} coeffB=${coeffB} steps=${steps}`,
    );
    await this.updateProgress(0);

    for (let i = 0; i < steps; i++) {
      value = await this.computeStep(
        value,
        coeffA,
        coeffB,
        id,
        i,
        i === failAtStep,
      );
      await this.log(`step ${i}: value=${value}`);
      await this.updateProgress(Math.floor(((i + 1) / steps) * 100));

      if (i === sleepAfterStep && sleepMs !== undefined) {
        const wakeAt = await this.scheduleSleep(sleepMs);
        this.breakUntil(wakeAt);
      }
    }

    await this.log(`done id=${id} final=${value}`);
    return { id, value, steps };
  }
}
