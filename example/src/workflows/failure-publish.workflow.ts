import { DozerWorkflow, NonRetryableError, Step, Workflow } from 'dozer';

@Workflow({
  name: 'failure-publish',
  resultQueue: {
    jobName: 'workflow-result',
    job: {
      removeOnComplete: false,
    },
    publishOnFailure: true,
  },
})
export class FailurePublishWorkflow extends DozerWorkflow<{ id: string }> {
  @Step({ name: 'fail' })
  fail(id: string): Promise<void> {
    return Promise.reject(
      new NonRetryableError(`failure-publish-error:${id}`),
    );
  }

  async run(input: { id: string }): Promise<void> {
    await this.fail(input.id);
  }
}
