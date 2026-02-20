import { Injectable } from '@nestjs/common';

type WorkflowVersion = 'v1' | 'v2';
type ExternalVariant = 'a' | 'b';

@Injectable()
export class ScenarioControlsService {
  private readonly timerTicks = new Map<string, number>();
  private readonly randomValues = new Map<string, number>();
  private readonly externalValues = new Map<string, ExternalVariant>();
  private readonly workflowVersions = new Map<string, WorkflowVersion>();

  setTimerTick(key: string, tick: number): void {
    this.timerTicks.set(key, tick);
  }

  getTimerTick(key: string): number {
    const value = this.timerTicks.get(key);
    return value ?? Date.now();
  }

  setRandomValue(key: string, value: number): void {
    this.randomValues.set(key, value);
  }

  getRandomValue(key: string): number {
    const value = this.randomValues.get(key);
    return value ?? Math.random();
  }

  setExternalValue(key: string, value: ExternalVariant): void {
    this.externalValues.set(key, value);
  }

  getExternalValue(key: string): ExternalVariant {
    return this.externalValues.get(key) ?? 'a';
  }

  setVersion(key: string, version: WorkflowVersion): void {
    this.workflowVersions.set(key, version);
  }

  getVersion(key: string): WorkflowVersion {
    return this.workflowVersions.get(key) ?? 'v1';
  }

  reset(): void {
    this.timerTicks.clear();
    this.randomValues.clear();
    this.externalValues.clear();
    this.workflowVersions.clear();
  }
}
