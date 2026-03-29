import { AsyncLocalStorage } from 'node:async_hooks';
import type { RetryOptions } from '../decorators/step.decorator';
import { NonDeterminismError } from '../errors/non-determinism.error';
import { WorkflowSleepRequestedError } from '../errors/workflow-sleep-requested.error';
import { WorkflowSignalWaitRequestedError } from '../errors/workflow-signal-wait-requested.error';
import { WorkflowStateStore } from './workflow-state.store';

interface StepFrame {
  path: number[];
  nextChildIndex: number;
}

export interface StepInvocation {
  key: string;
  hasCachedResult: boolean;
  cachedResult?: unknown;
}

interface WorkflowExecutionContextOptions {
  defaultRetry?: RetryOptions;
  requireCachedSteps?: boolean;
}

const toStepKey = (path: number[], stepName: string): string => {
  return `${path.join('.')}:${stepName}`;
};

const toPathPrefix = (path: number[]): string => {
  return `${path.join('.')}.`;
};

const extractStepPath = (stepKey: string): string => {
  const separatorIndex = stepKey.indexOf(':');
  if (separatorIndex === -1) {
    return stepKey;
  }

  return stepKey.slice(0, separatorIndex);
};

export class WorkflowExecutionContext {
  private readonly frames: StepFrame[] = [{ path: [], nextChildIndex: 0 }];
  private traceCursor = 0;

  constructor(
    private readonly stateStore: WorkflowStateStore,
    private readonly options: WorkflowExecutionContextOptions = {},
  ) {}

  private skipCachedDescendants(path: number[]): void {
    const pathPrefix = toPathPrefix(path);

    while (true) {
      const nextStepKey = this.stateStore.getTraceStepKey(this.traceCursor);
      if (!nextStepKey) {
        return;
      }

      const nextPath = extractStepPath(nextStepKey);
      if (!nextPath.startsWith(pathPrefix)) {
        return;
      }

      this.traceCursor += 1;
    }
  }

  async enterStep(stepName: string): Promise<StepInvocation> {
    const parent = this.frames[this.frames.length - 1];
    if (!parent) {
      throw new Error('Workflow frame stack is empty.');
    }

    const path = [...parent.path, parent.nextChildIndex];
    parent.nextChildIndex += 1;

    const key = toStepKey(path, stepName);
    await this.stateStore.beginStep(this.traceCursor, key);
    this.traceCursor += 1;

    this.frames.push({
      path,
      nextChildIndex: 0,
    });

    const hasCachedResult = this.stateStore.hasStep(key);
    if (hasCachedResult) {
      this.skipCachedDescendants(path);
    } else if (this.options.requireCachedSteps) {
      throw new NonDeterminismError(
        `Determinism probe expected cached step "${key}", but no cached result was found.`,
      );
    }

    return {
      key,
      hasCachedResult,
      cachedResult: hasCachedResult
        ? this.stateStore.getStepResult(key)
        : undefined,
    };
  }

  resetCurrentStepChildren(): void {
    const current = this.frames[this.frames.length - 1];
    if (!current) {
      throw new Error('Workflow frame stack is empty.');
    }

    current.nextChildIndex = 0;
  }

  async completeStep(stepKey: string, result: unknown): Promise<void> {
    await this.stateStore.saveStepResult(stepKey, result);
  }

  getStepRetryCount(stepKey: string): number {
    return this.stateStore.getStepRetryCount(stepKey);
  }

  async incrementStepRetryCount(stepKey: string): Promise<number> {
    return this.stateStore.incrementStepRetryCount(stepKey);
  }

  exitStep(): void {
    if (this.frames.length === 1) {
      return;
    }

    this.frames.pop();
  }

  getTraceCursor(): number {
    return this.traceCursor;
  }

  getDefaultRetry(): RetryOptions | undefined {
    return this.options.defaultRetry;
  }

  async sleep(durationMs: number): Promise<void> {
    const invocation = await this.enterStep('__sleep__');

    if (invocation.hasCachedResult) {
      this.exitStep();
      return;
    }

    const existingWakeUpAt = this.stateStore.getSleepIntent(invocation.key);
    if (existingWakeUpAt !== undefined) {
      if (Date.now() >= existingWakeUpAt) {
        await this.stateStore.completeSleep(invocation.key);
        this.exitStep();
        return;
      }
      this.exitStep();
      throw new WorkflowSleepRequestedError(existingWakeUpAt);
    }

    const wakeUpAt = Date.now() + durationMs;
    await this.stateStore.saveSleepIntent(invocation.key, wakeUpAt);
    this.exitStep();
    throw new WorkflowSleepRequestedError(wakeUpAt);
  }

  async waitForSignal<T>(
    signalName: string,
    opts?: { timeoutMs?: number },
  ): Promise<T | null> {
    const invocation = await this.enterStep(`__signal__:${signalName}`);

    if (invocation.hasCachedResult) {
      this.exitStep();
      return invocation.cachedResult as T | null;
    }

    const pending = this.stateStore.getPendingSignal(signalName);
    if (pending) {
      if (pending.expiresAt !== undefined && Date.now() >= pending.expiresAt) {
        await this.stateStore.saveStepResult(invocation.key, null);
        await this.stateStore.clearPendingSignal(signalName);
        this.exitStep();
        return null;
      }
      this.exitStep();
      throw new WorkflowSignalWaitRequestedError(signalName, pending.expiresAt);
    }

    const expiresAt =
      opts?.timeoutMs !== undefined ? Date.now() + opts.timeoutMs : undefined;
    await this.stateStore.savePendingSignal(
      signalName,
      invocation.key,
      expiresAt,
    );
    this.exitStep();
    throw new WorkflowSignalWaitRequestedError(signalName, expiresAt);
  }
}

const executionContextStorage =
  new AsyncLocalStorage<WorkflowExecutionContext>();

export class WorkflowExecutionContextStorage {
  static run<T>(context: WorkflowExecutionContext, callback: () => T): T {
    return executionContextStorage.run(context, callback);
  }

  static get(): WorkflowExecutionContext | undefined {
    return executionContextStorage.getStore();
  }
}
