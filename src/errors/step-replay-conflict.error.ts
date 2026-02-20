import { DozerError } from './dozer.error';

export class StepReplayConflictError extends DozerError {
  constructor(
    public readonly traceIndex: number,
    public readonly expectedStepKey: string,
    public readonly actualStepKey: string,
  ) {
    super(
      'STEP_REPLAY_CONFLICT',
      `Step replay conflict at trace index ${traceIndex}: expected "${expectedStepKey}", got "${actualStepKey}".`,
    );
  }
}
