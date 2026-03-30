import { DozerWorkflow, Step, Workflow } from 'dozer';

export type JobRuntimeInput = {
  steps: number;
  priorityChange?: number;
  clearAfterFirstStep?: boolean;
};

@Workflow({ name: 'job-runtime' })
export class JobRuntimeWorkflow extends DozerWorkflow<JobRuntimeInput> {
  @Step({ name: 'step-a' })
  stepA(steps: number): Promise<string> {
    return Promise.resolve(`step-a-${steps}`);
  }

  @Step({ name: 'step-b' })
  stepB(prev: string): Promise<string> {
    return Promise.resolve(`${prev}+step-b`);
  }

  async run(input: JobRuntimeInput): Promise<{ result: string }> {
    await this.log('workflow started');
    await this.updateProgress(0);

    const a = await this.stepA(input.steps);
    await this.log(`step-a done: ${a}`);
    await this.updateProgress(50);

    if (input.priorityChange !== undefined) {
      await this.changePriority({ priority: input.priorityChange });
    }

    if (input.clearAfterFirstStep) {
      await this.clearLogs();
    }

    const b = await this.stepB(a);
    await this.log(`step-b done: ${b}`);
    await this.updateProgress(100);

    return { result: b };
  }
}
