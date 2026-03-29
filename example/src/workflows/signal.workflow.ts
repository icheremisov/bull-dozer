import { DozerWorkflow, Step, Workflow } from 'dozer';

export interface SignalWorkflowResult {
  approved: boolean;
  payload: unknown;
}

@Workflow({ name: 'signal-workflow' })
export class SignalWorkflow extends DozerWorkflow<{
  id: string;
  timeoutMs?: number;
}> {
  @Step({ name: 'prepare' })
  prepare(id: string): Promise<{ prepared: true; id: string }> {
    return Promise.resolve({ prepared: true, id });
  }

  async run(input: {
    id: string;
    timeoutMs?: number;
  }): Promise<SignalWorkflowResult> {
    await this.prepare(input.id);
    const payload = await this.waitForSignal<unknown>('approval', {
      timeoutMs: input.timeoutMs,
    });
    return {
      approved: payload !== null,
      payload,
    };
  }
}
