import { WorkflowExecutionContext, WorkflowExecutionContextStorage } from '../runtime/workflow-execution-context';

export abstract class DozerWorkflow<TInput = unknown> {
  abstract run(input: TInput): Promise<unknown>;

  protected async sleep(durationMs: number): Promise<void> {
    const context = WorkflowExecutionContextStorage.get() as WorkflowExecutionContext | undefined;
    if (!context) {
      await new Promise<void>((resolve) => setTimeout(resolve, durationMs));
      return;
    }
    await context.sleep(durationMs);
  }

  protected async sleepUntil(timestamp: number): Promise<void> {
    return this.sleep(Math.max(0, timestamp - Date.now()));
  }

  protected async waitForSignal<T>(
    signalName: string,
    opts?: { timeoutMs?: number },
  ): Promise<T | null> {
    const context = WorkflowExecutionContextStorage.get() as WorkflowExecutionContext | undefined;
    if (!context) {
      throw new Error('waitForSignal() must be called within a workflow context');
    }
    return context.waitForSignal(signalName, opts);
  }
}
