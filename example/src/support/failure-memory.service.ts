import { Injectable } from '@nestjs/common';

@Injectable()
export class FailureMemoryService {
  private readonly counters = new Map<string, number>();

  markAndShouldFail(key: string, failTimes: number): boolean {
    const current = this.counters.get(key) ?? 0;
    this.counters.set(key, current + 1);
    return current < failTimes;
  }

  calls(key: string): number {
    return this.counters.get(key) ?? 0;
  }

  reset(): void {
    this.counters.clear();
  }
}
