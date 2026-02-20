import { DozerError } from './dozer.error';

export class SerializationError extends DozerError {
  constructor(message: string, cause?: unknown) {
    super('SERIALIZATION_ERROR', message);
    if (cause !== undefined) {
      (this as Error & { cause?: unknown }).cause = cause;
    }
  }
}
