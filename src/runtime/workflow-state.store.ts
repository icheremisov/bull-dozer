import { DOZER_JOB_STATE_KEY } from '../constants';
import { NonDeterminismError } from '../errors/non-determinism.error';
import { SerializationError } from '../errors/serialization.error';
import { StepReplayConflictError } from '../errors/step-replay-conflict.error';
import {
  CompactWorkflowState,
  WORKFLOW_STATUS,
  WorkflowJob,
  WorkflowJobData,
} from '../queue/workflow-queue';
import {
  deserializeFromStorage,
  serializeForStorage,
} from './value-serializer';

const createInitialState = (): CompactWorkflowState => ({
  s: WORKFLOW_STATUS.pending,
  c: {},
  a: {},
  t: [],
});

interface WorkflowStateStoreOptions {
  readOnly?: boolean;
  strictTrace?: boolean;
}

const asErrorMessage = (error: unknown): string => {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
};

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === 'object' && value !== null;
};

const isWorkflowStatusCode = (
  value: unknown,
): value is CompactWorkflowState['s'] => {
  return (
    value === WORKFLOW_STATUS.pending ||
    value === WORKFLOW_STATUS.running ||
    value === WORKFLOW_STATUS.failed ||
    value === WORKFLOW_STATUS.completed ||
    value === WORKFLOW_STATUS.cancelled ||
    value === WORKFLOW_STATUS.completing
  );
};

const isCompactWorkflowState = (
  value: unknown,
): value is CompactWorkflowState => {
  if (!isRecord(value)) {
    return false;
  }

  return (
    isWorkflowStatusCode(value.s) &&
    isRecord(value.c) &&
    (value.a === undefined || isRecord(value.a)) &&
    (value.u === undefined || isRecord(value.u)) &&
    (value.sl === undefined || isRecord(value.sl)) &&
    (value.ps === undefined || isRecord(value.ps)) &&
    Array.isArray(value.t)
  );
};

const extractStepPath = (stepKey: string): string => {
  const separatorIndex = stepKey.indexOf(':');
  if (separatorIndex === -1) {
    return stepKey;
  }

  return stepKey.slice(0, separatorIndex);
};

export class WorkflowStateStore<TInput = unknown> {
  private state: CompactWorkflowState;

  constructor(
    private readonly job: WorkflowJob<TInput>,
    private readonly options: WorkflowStateStoreOptions = {},
  ) {
    const persistedState = this.job.data[DOZER_JOB_STATE_KEY];
    this.state = isCompactWorkflowState(persistedState)
      ? persistedState
      : createInitialState();
  }

  hasStep(stepKey: string): boolean {
    return (
      stepKey in this.state.c ||
      Boolean(this.state.u && stepKey in this.state.u)
    );
  }

  getStepResult(stepKey: string): unknown {
    if (this.state.u && stepKey in this.state.u) {
      return undefined;
    }
    const result = deserializeFromStorage(this.state.c[stepKey]);
    return result;
  }

  async markRunning(): Promise<void> {
    this.state.s = WORKFLOW_STATUS.running;
    this.state.e = undefined;
    await this.flush();
  }

  async markCompleted(result: unknown): Promise<void> {
    this.state.s = WORKFLOW_STATUS.completed;
    this.state.r = await serializeForStorage(result, 'workflow result');
    this.state.e = undefined;
    this.compactDescendantStepCache();
    await this.flush();
  }

  async markPublishingResult(result: unknown): Promise<void> {
    this.state.s = WORKFLOW_STATUS.completing;
    this.state.r = await serializeForStorage(result, 'workflow result');
    this.state.e = undefined;
    await this.flush();
  }

  async markCompletedFromStoredResult(): Promise<void> {
    this.state.s = WORKFLOW_STATUS.completed;
    this.state.e = undefined;
    this.compactDescendantStepCache();
    await this.flush();
  }

  async markFailed(error: unknown): Promise<void> {
    this.state.s = WORKFLOW_STATUS.failed;
    this.state.r = undefined;
    this.state.e = asErrorMessage(error);
    await this.flush();
  }

  async markCancelled(message = 'Workflow cancelled'): Promise<void> {
    this.state.s = WORKFLOW_STATUS.cancelled;
    this.state.r = undefined;
    this.state.e = message;
    await this.flush();
  }

  private removeDescendantStepCache(stepKey: string): void {
    const pathPrefix = `${extractStepPath(stepKey)}.`;

    for (const cachedKey of Object.keys(this.state.c)) {
      if (extractStepPath(cachedKey).startsWith(pathPrefix)) {
        delete this.state.c[cachedKey];
      }
    }

    if (this.state.u) {
      for (const cachedKey of Object.keys(this.state.u)) {
        if (extractStepPath(cachedKey).startsWith(pathPrefix)) {
          delete this.state.u[cachedKey];
        }
      }

      if (Object.keys(this.state.u).length === 0) {
        this.state.u = undefined;
      }
    }

    if (this.state.a) {
      for (const cachedKey of Object.keys(this.state.a)) {
        if (extractStepPath(cachedKey).startsWith(pathPrefix)) {
          delete this.state.a[cachedKey];
        }
      }

      if (Object.keys(this.state.a).length === 0) {
        this.state.a = undefined;
      }
    }
  }

  private compactDescendantStepCache(): void {
    const stepKeys = [
      ...Object.keys(this.state.c),
      ...Object.keys(this.state.u ?? {}),
    ].sort((left, right) => {
      return (
        extractStepPath(left).split('.').length -
        extractStepPath(right).split('.').length
      );
    });

    for (const stepKey of stepKeys) {
      if (
        !(stepKey in this.state.c) &&
        !(this.state.u && stepKey in this.state.u)
      ) {
        continue;
      }

      this.removeDescendantStepCache(stepKey);
    }
  }

  async saveStepResult(stepKey: string, result: unknown): Promise<void> {
    if (result === undefined) {
      this.state.u = this.state.u ?? {};
      this.state.u[stepKey] = 1;
      delete this.state.c[stepKey];
    } else {
      this.state.c[stepKey] = await serializeForStorage(
        result,
        `step result "${stepKey}"`,
      );
      if (this.state.u) {
        delete this.state.u[stepKey];
      }
    }
    if (this.state.a) {
      delete this.state.a[stepKey];
      if (Object.keys(this.state.a).length === 0) {
        this.state.a = undefined;
      }
    }

    // Parent step result supersedes all nested descendant step caches.
    this.removeDescendantStepCache(stepKey);
    await this.flush();
  }

  getStepRetryCount(stepKey: string): number {
    const value = this.state.a?.[stepKey];
    return typeof value === 'number' && Number.isFinite(value) ? value : 0;
  }

  async incrementStepRetryCount(stepKey: string): Promise<number> {
    this.state.a = this.state.a ?? {};
    const nextValue = this.getStepRetryCount(stepKey) + 1;
    this.state.a[stepKey] = nextValue;
    await this.flush();
    return nextValue;
  }

  saveSleepIntent(stepKey: string, wakeUpAt: number): Promise<void> {
    this.state.sl = this.state.sl ?? {};
    this.state.sl[stepKey] = wakeUpAt;
    return this.flush();
  }

  getSleepIntent(stepKey: string): number | undefined {
    return this.state.sl?.[stepKey];
  }

  async completeSleep(stepKey: string): Promise<void> {
    if (this.state.sl) {
      delete this.state.sl[stepKey];
      if (Object.keys(this.state.sl).length === 0) {
        this.state.sl = undefined;
      }
    }
    this.state.u = this.state.u ?? {};
    this.state.u[stepKey] = 1;
    await this.flush();
  }

  savePendingSignal(
    signalName: string,
    stepKey: string,
    expiresAt?: number,
  ): Promise<void> {
    this.state.ps = this.state.ps ?? {};
    this.state.ps[signalName] = expiresAt !== undefined
      ? { k: stepKey, e: expiresAt }
      : { k: stepKey };
    return this.flush();
  }

  getPendingSignal(
    signalName: string,
  ): { stepKey: string; expiresAt?: number } | undefined {
    const entry = this.state.ps?.[signalName];
    if (!entry) return undefined;
    return { stepKey: entry.k, expiresAt: entry.e };
  }

  async clearPendingSignal(signalName: string): Promise<void> {
    if (this.state.ps) {
      delete this.state.ps[signalName];
      if (Object.keys(this.state.ps).length === 0) {
        this.state.ps = undefined;
      }
    }
    await this.flush();
  }

  async deliverSignal(signalName: string, payload: unknown): Promise<boolean> {
    const pending = this.getPendingSignal(signalName);
    if (!pending) return false;

    await this.saveStepResult(pending.stepKey, payload);
    if (this.state.ps) {
      delete this.state.ps[signalName];
      if (Object.keys(this.state.ps).length === 0) {
        this.state.ps = undefined;
      }
    }
    await this.flush();
    return true;
  }

  async beginStep(traceIndex: number, stepKey: string): Promise<void> {
    const expected = this.state.t[traceIndex];
    if (expected === undefined) {
      if (this.options.strictTrace) {
        throw new NonDeterminismError(
          `Workflow trace diverged: unexpected step "${stepKey}" at trace index ${traceIndex}.`,
        );
      }
      this.state.t[traceIndex] = stepKey;
      await this.flush();
      return;
    }

    if (expected !== stepKey) {
      throw new StepReplayConflictError(traceIndex, expected, stepKey);
    }
  }

  assertTraceConsumed(consumedTraceLength: number): void {
    const expectedLength = this.state.t.length;
    if (consumedTraceLength !== expectedLength) {
      throw new NonDeterminismError(
        `Workflow trace diverged: consumed ${consumedTraceLength} step(s), expected ${expectedLength}.`,
      );
    }
  }

  getTraceStepKey(traceIndex: number): string | undefined {
    const stepKey = this.state.t[traceIndex];
    if (typeof stepKey !== 'string') {
      return undefined;
    }

    return stepKey;
  }

  getFinalResultSerialized(): unknown {
    return this.state.r;
  }

  private async flush(): Promise<void> {
    if (this.options.readOnly) {
      return;
    }

    try {
      const nextData: WorkflowJobData<TInput> = {
        ...this.job.data,
        [DOZER_JOB_STATE_KEY]: this.state,
      };
      await this.job.updateData(nextData);
    } catch (error) {
      throw new SerializationError(
        'Failed to persist workflow state into job data.',
        error,
      );
    }
  }
}
