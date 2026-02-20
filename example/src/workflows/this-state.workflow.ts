import { Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

interface RuntimeState {
  token: string;
}

@Workflow({ name: 'this-state' })
export class ThisStateWorkflow {
  current?: RuntimeState;

  constructor(private readonly failureMemory: FailureMemoryService) {}

  @Step({ name: 'hydrate' })
  hydrate(input: { id: string; value: number }): Promise<RuntimeState> {
    this.failureMemory.markAndShouldFail(`this-state:hydrate:${input.id}`, 0);
    const state: RuntimeState = {
      token: `${input.id}:${input.value}`,
    };
    this.current = state;
    return Promise.resolve(state);
  }

  @Step({ name: 'verify' })
  verify(input: { id: string }): Promise<{ ok: true; token: string }> {
    if (!this.current) {
      throw new Error('missing-this-state');
    }

    const shouldFail = this.failureMemory.markAndShouldFail(
      `this-state:verify:${input.id}`,
      1,
    );
    if (shouldFail) {
      throw new Error('this-state-fail-once');
    }

    return Promise.resolve({
      ok: true,
      token: this.current.token,
    });
  }

  async run(input: {
    id: string;
    value: number;
  }): Promise<{ ok: true; token: string }> {
    const state = await this.hydrate(input);
    this.current = state;
    return this.verify({ id: input.id });
  }
}
