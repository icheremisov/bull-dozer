import { Step, Workflow } from 'dozer';

@Workflow({ name: 'step-outside-flow' })
export class StepOutsideFlowWorkflow {
  private readonly constructorValue: number;

  constructor() {
    void this.seedFromConstructor();
    this.constructorValue = 5;
  }

  @Step({ name: 'seed-from-constructor' })
  seedFromConstructor(): number {
    return 5;
  }

  @Step({ name: 'increment' })
  increment(value: number): number {
    return value + 1;
  }

  run(): Promise<number> {
    return Promise.resolve(this.increment(this.constructorValue));
  }
}
