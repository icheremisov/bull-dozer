import { Injectable } from '@nestjs/common';

const normalizedHash = (value: string): number => {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }

  return (hash >>> 0) / 4294967295;
};

@Injectable()
export class PerfFailureService {
  private readonly counters = new Map<string, number>();

  shouldFailOnce(key: string, failureRate: number): boolean {
    const rate = Math.min(1, Math.max(0, failureRate));
    const shouldFail = normalizedHash(key) < rate;
    if (!shouldFail) {
      return false;
    }

    const current = this.counters.get(key) ?? 0;
    this.counters.set(key, current + 1);
    return current === 0;
  }
}
