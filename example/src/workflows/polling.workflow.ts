import { DozerWorkflow, Step, Workflow } from 'dozer';
import { ScenarioControlsService } from '../support/scenario-controls.service';

/**
 * Demonstrates the polling pattern: a @Step with a while-loop that calls
 * breakFor() to pause between polls without growing the workflow trace.
 *
 * The step body re-executes on each resume. Only one trace entry is written
 * regardless of how many polling iterations are needed.
 */
@Workflow({ name: 'polling-workflow' })
export class PollingWorkflow extends DozerWorkflow<{
  id: string;
  pollIntervalMs?: number;
}> {
  constructor(private readonly controls: ScenarioControlsService) {
    super();
  }

  @Step({ name: 'poll-status' })
  pollStatus(input: { id: string; intervalMs: number }): Promise<string> {
    while (true) {
      const status = this.controls.nextPollingStatus(input.id);
      if (status !== 'pending') return Promise.resolve(status);
      this.breakFor(input.intervalMs);
    }
  }

  async run(input: {
    id: string;
    pollIntervalMs?: number;
  }): Promise<{ status: string }> {
    const status = await this.pollStatus({
      id: input.id,
      intervalMs: input.pollIntervalMs ?? 200,
    });
    return { status };
  }
}
