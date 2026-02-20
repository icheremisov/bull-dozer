import { DozerError } from './dozer.error';

export class NonDeterminismError extends DozerError {
  constructor(message: string) {
    super('NON_DETERMINISM', message);
  }
}
