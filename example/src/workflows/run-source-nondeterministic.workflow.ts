import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';
import { ScenarioControlsService } from '../support/scenario-controls.service';

type SourceKind = 'timer' | 'random' | 'external';

@Workflow({ name: 'run-source-nondeterministic' })
export class RunSourceNondeterministicWorkflow extends DozerWorkflow<{ id: string; source: SourceKind; value: number }> {
  constructor(
    private readonly controls: ScenarioControlsService,
    private readonly failureMemory: FailureMemoryService,
  ) {
    super();
  }

  @Step({ name: 'timer-even' })
  timerEven(value: number): Promise<number> {
    return Promise.resolve(value + 2);
  }

  @Step({ name: 'timer-odd' })
  timerOdd(value: number): Promise<number> {
    return Promise.resolve(value + 3);
  }

  @Step({ name: 'random-low' })
  randomLow(value: number): Promise<number> {
    return Promise.resolve(value + 4);
  }

  @Step({ name: 'random-high' })
  randomHigh(value: number): Promise<number> {
    return Promise.resolve(value + 5);
  }

  @Step({ name: 'external-a' })
  externalA(value: number): Promise<number> {
    return Promise.resolve(value + 6);
  }

  @Step({ name: 'external-b' })
  externalB(value: number): Promise<number> {
    return Promise.resolve(value + 7);
  }

  @Step({ name: 'fail-once' })
  failOnce(key: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `run-source-nondet:${key}`,
      1,
    );
    if (shouldFail) {
      throw new Error('run-source-nondet-fail-once');
    }

    return Promise.resolve();
  }

  async run(input: {
    id: string;
    source: SourceKind;
    value: number;
  }): Promise<{ value: number }> {
    let value = input.value;

    if (input.source === 'timer') {
      const tick = this.controls.getTimerTick(input.id);
      value =
        tick % 2 === 0
          ? await this.timerEven(input.value)
          : await this.timerOdd(input.value);
    } else if (input.source === 'random') {
      const random = this.controls.getRandomValue(input.id);
      value =
        random < 0.5
          ? await this.randomLow(input.value)
          : await this.randomHigh(input.value);
    } else {
      const external = this.controls.getExternalValue(input.id);
      value =
        external === 'a'
          ? await this.externalA(input.value)
          : await this.externalB(input.value);
    }

    await this.failOnce(`${input.source}:${input.id}`);
    return { value };
  }
}
