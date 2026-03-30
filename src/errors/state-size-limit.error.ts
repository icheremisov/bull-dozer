import { DozerError } from './dozer.error';

export class StateSizeLimitError extends DozerError {
  constructor(
    public readonly actualBytes: number,
    public readonly limitBytes: number,
  ) {
    super(
      'STATE_SIZE_LIMIT_EXCEEDED',
      `Workflow state size (${actualBytes} bytes) exceeds limit of ${limitBytes} bytes.`,
    );
  }
}
