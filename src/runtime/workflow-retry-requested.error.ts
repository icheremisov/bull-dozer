export class WorkflowRetryRequestedError extends Error {
  constructor(
    public readonly stepKey: string,
    public readonly failedAttempts: number,
    public readonly maxAttempts: number,
    public readonly backoffMs: number,
    public readonly causeError: unknown,
  ) {
    super(
      `Workflow retry requested for step "${stepKey}": ${failedAttempts}/${maxAttempts} failed attempt(s).`,
    );
  }
}
