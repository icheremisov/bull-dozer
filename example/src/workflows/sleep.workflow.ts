import { DozerWorkflow, Step, Workflow } from 'dozer';

@Workflow({ name: 'sleep-workflow' })
export class SleepWorkflow extends DozerWorkflow<{
  id: string;
  durationMs: number;
  value: number;
}> {
  @Step({ name: 'prepare' })
  prepare(input: { id: string; value: number }): Promise<number> {
    return Promise.resolve(input.value);
  }

  /**
   * Captures the wake-up timestamp as a cached step so replay is deterministic.
   * Without this, Date.now() + durationMs would shift on every replay run.
   */
  @Step({ name: 'schedule-resume' })
  scheduleResume(durationMs: number): Promise<number> {
    return Promise.resolve(Date.now() + durationMs);
  }

  @Step({ name: 'process' })
  process(value: number): Promise<number> {
    return Promise.resolve(value + 1);
  }

  async run(input: {
    id: string;
    durationMs: number;
    value: number;
  }): Promise<{ value: number }> {
    const prepared = await this.prepare(input);
    const wakeUpAt = await this.scheduleResume(input.durationMs);
    this.breakUntil(wakeUpAt);
    const processed = await this.process(prepared);
    return { value: processed };
  }
}
