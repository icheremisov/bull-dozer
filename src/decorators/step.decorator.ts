import 'reflect-metadata';
import { STEP_OPTIONS_METADATA } from '../constants';
import { NonRetryableError } from '../errors/non-retryable.error';
import { TimeoutError } from '../errors/timeout.error';
import { WorkflowExecutionContextStorage } from '../runtime/workflow-execution-context';
import { BackoffStrategy, resolveRetryDelayMs } from '../runtime/retry-policy';
import { WorkflowRetryRequestedError } from '../runtime/workflow-retry-requested.error';

export interface RetryOptions {
  attempts?: number;
  delayMs?: number;
  strategy?: BackoffStrategy;
  backoffMs?: number;
}

export interface StepOptions {
  name?: string;
  retry?: RetryOptions;
  timeout?: number;
  timeoutMs?: number;
}

const runWithTimeout = async (
  value: unknown,
  stepName: string,
  timeoutMs: number,
): Promise<unknown> => {
  const timeout = Math.max(1, Math.floor(timeoutMs));
  if (!Number.isFinite(timeout) || timeout <= 0) {
    return Promise.resolve(value);
  }

  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      Promise.resolve(value),
      new Promise<never>((_resolve, reject) => {
        timeoutId = setTimeout(() => {
          reject(new TimeoutError(stepName, timeout));
        }, timeout);
      }),
    ]);
  } finally {
    if (timeoutId !== undefined) {
      clearTimeout(timeoutId);
    }
  }
};

export function Step(options: StepOptions = {}): MethodDecorator {
  return <T>(
    _target: object,
    key: string | symbol,
    descriptor: TypedPropertyDescriptor<T>,
  ) => {
    type StepMethod = (this: unknown, ...args: unknown[]) => unknown;

    const original = descriptor.value;
    if (typeof original !== 'function') {
      throw new Error(
        `@Step() can only decorate methods, got "${String(key)}".`,
      );
    }
    const method = original as unknown as StepMethod;

    const stepName = options.name ?? String(key);
    const normalizedOptions: StepOptions = {
      ...options,
      name: stepName,
    };

    Reflect.defineMetadata(STEP_OPTIONS_METADATA, normalizedOptions, method);

    const wrapped: StepMethod = async function dozerStepWrapper(
      this: unknown,
      ...args: unknown[]
    ): Promise<unknown> {
      const context = WorkflowExecutionContextStorage.get();
      if (!context) {
        const executed = method.call(this, ...args) as unknown;
        return Promise.resolve(executed);
      }

      const invocation = await context.enterStep(stepName);
      if (invocation.hasCachedResult) {
        context.exitStep();
        return invocation.cachedResult;
      }

      const defaultRetry = context.getDefaultRetry();
      const maxAttempts = Math.max(
        1,
        normalizedOptions.retry?.attempts ?? defaultRetry?.attempts ?? 1,
      );
      const backoffBaseMs =
        normalizedOptions.retry?.delayMs ??
        normalizedOptions.retry?.backoffMs ??
        defaultRetry?.delayMs ??
        defaultRetry?.backoffMs ??
        0;
      const backoffStrategy =
        normalizedOptions.retry?.strategy ??
        defaultRetry?.strategy ??
        'constant';
      const timeoutMs =
        normalizedOptions.timeoutMs ?? normalizedOptions.timeout ?? 0;
      try {
        context.resetCurrentStepChildren();
        const executed = method.call(this, ...args) as unknown;
        const result: unknown =
          timeoutMs > 0
            ? await runWithTimeout(executed, stepName, timeoutMs)
            : await Promise.resolve(executed);
        await context.completeStep(invocation.key, result);
        return result;
      } catch (error) {
        if (error instanceof NonRetryableError) {
          throw error;
        }

        const failedAttempts = context.getStepRetryCount(invocation.key) + 1;
        if (failedAttempts < maxAttempts) {
          await context.incrementStepRetryCount(invocation.key);
          const backoffMs = resolveRetryDelayMs(
            backoffBaseMs,
            backoffStrategy,
            failedAttempts,
          );
          throw new WorkflowRetryRequestedError(
            invocation.key,
            failedAttempts,
            maxAttempts,
            backoffMs,
            error,
          );
        }

        throw error;
      } finally {
        context.exitStep();
      }
    };
    descriptor.value = wrapped as unknown as T;

    Reflect.defineMetadata(STEP_OPTIONS_METADATA, normalizedOptions, wrapped);
    return descriptor;
  };
}
