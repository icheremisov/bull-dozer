import { DozerError } from './dozer.error';

export class TimeoutError extends DozerError {
  constructor(
    public readonly stepName: string,
    public readonly timeoutMs: number,
  ) {
    super('STEP_TIMEOUT', `Step "${stepName}" timed out after ${timeoutMs}ms.`);
  }
}
