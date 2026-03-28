export interface WorkflowWithFailureHandler<TInput = unknown> {
  onFailed(error: Error, input: TInput, jobId: string): Promise<void>;
}
