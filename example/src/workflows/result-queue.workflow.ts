import { Workflow } from 'dozer';

@Workflow({
  name: 'result-queue',
  resultQueue: {
    jobName: 'workflow-result',
    job: {
      removeOnComplete: false,
    },
  },
})
export class ResultQueueWorkflow {
  run(input: { value: number }): Promise<{ value: number; ok: true }> {
    return Promise.resolve({
      ok: true,
      value: input.value + 1,
    });
  }
}
