import { resolveRetryDelayMs } from './retry-policy';

describe('retry-policy', () => {
  it('computes constant strategy delay', () => {
    expect(resolveRetryDelayMs(2000, 'constant', 1)).toBe(2000);
    expect(resolveRetryDelayMs(2000, 'constant', 2)).toBe(2000);
    expect(resolveRetryDelayMs(2000, 'constant', 3)).toBe(2000);
  });

  it('computes linear strategy delay', () => {
    expect(resolveRetryDelayMs(2000, 'linear', 1)).toBe(2000);
    expect(resolveRetryDelayMs(2000, 'linear', 2)).toBe(4000);
    expect(resolveRetryDelayMs(2000, 'linear', 3)).toBe(6000);
    expect(resolveRetryDelayMs(2000, 'linear', 4)).toBe(8000);
  });

  it('computes exponential strategy delay', () => {
    expect(resolveRetryDelayMs(2000, 'exponential', 1)).toBe(2000);
    expect(resolveRetryDelayMs(2000, 'exponential', 2)).toBe(4000);
    expect(resolveRetryDelayMs(2000, 'exponential', 3)).toBe(8000);
    expect(resolveRetryDelayMs(2000, 'exponential', 4)).toBe(16000);
  });
});
