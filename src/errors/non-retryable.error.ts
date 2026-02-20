import { DozerError } from './dozer.error';

export class NonRetryableError extends DozerError {
  constructor(
    message: string,
    public readonly causeError?: unknown,
  ) {
    super('NON_RETRYABLE', message);
  }
}
