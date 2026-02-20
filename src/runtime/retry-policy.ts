export type BackoffStrategy = 'constant' | 'linear' | 'exponential';

export const resolveRetryDelayMs = (
  baseDelayMs: number,
  strategy: BackoffStrategy,
  attempt: number,
): number => {
  const normalizedBase = Math.max(0, Math.floor(baseDelayMs));
  const normalizedAttempt = Math.max(1, Math.floor(attempt));

  if (normalizedBase === 0) {
    return 0;
  }

  if (strategy === 'linear') {
    return normalizedBase * normalizedAttempt;
  }

  if (strategy === 'exponential') {
    return normalizedBase * 2 ** (normalizedAttempt - 1);
  }

  return normalizedBase;
};
