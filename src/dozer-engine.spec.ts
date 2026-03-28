import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  BullMQQueueLike,
  DOZER_JOB_INPUT_KEY,
  DOZER_JOB_STATE_KEY,
  createWorkflowResultProcessor,
  decodeWorkflowResultJob,
  DozerClient,
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  NonDeterminismError,
  NonRetryableError,
  SerializationError,
  Step,
  StepReplayConflictError,
  TimeoutError,
  toWorkflowResultQueueJobId,
  WORKFLOW_STATUS,
  Workflow,
  WorkflowCancelledError,
  WorkflowJobOptions,
  WorkflowResultQueueJobData,
} from './index';

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

type RecoveryPayload = Record<string, unknown>;

@Injectable()
class RecoveryStats {
  validate = 0;
  process = 0;
  store = 0;
}

@Injectable()
class BranchService {
  branch: 'left' | 'right' = 'left';
}

@Injectable()
class FailOnceService {
  private readonly map = new Map<string, number>();

  shouldFail(key: string, failTimes = 1): boolean {
    const current = this.map.get(key) ?? 0;
    this.map.set(key, current + 1);
    return current < failTimes;
  }

  calls(key: string): number {
    return this.map.get(key) ?? 0;
  }

  reset(): void {
    this.map.clear();
  }
}

@Injectable()
class BinaryStats {
  inspected = 0;
  produced = 0;
}

@Injectable()
class NestedReplayStats {
  outer = 0;
  inner = 0;
  fail = 0;
}

@Injectable()
class DeterminismProbeStats {
  computeCalls = 0;
}

@Injectable()
class TimeoutCompensationStats {
  timedOut = 0;
  cleanup = 0;
}

@Injectable()
class WorkflowAutoResumeStats {
  prepare = 0;
  unstable = 0;
}

class CapturingResultQueue implements BullMQQueueLike<
  WorkflowResultQueueJobData<unknown>
> {
  readonly added: Array<{
    name: string;
    data: WorkflowResultQueueJobData<unknown>;
    options?: WorkflowJobOptions;
  }> = [];

  add(
    name: string,
    data: WorkflowResultQueueJobData<unknown>,
    options?: WorkflowJobOptions,
  ) {
    this.added.push({ name, data, options });
    return Promise.resolve({
      id: this.added.length,
      name,
      data,
      updateData: (
        nextData: WorkflowResultQueueJobData<unknown>,
      ): Promise<void> => {
        const index = this.added.length - 1;
        this.added[index] = { name, data: nextData, options };
        return Promise.resolve();
      },
    });
  }

  getJob(_jobId?: string): Promise<{
    id: string | number;
    name: string;
    data: WorkflowResultQueueJobData<unknown>;
    updateData: (nextData: WorkflowResultQueueJobData<unknown>) => Promise<void>;
  } | null> {
    return Promise.resolve(null);
  }
}

class FailOnceResultQueue extends CapturingResultQueue {
  private failed = false;

  override add(
    name: string,
    data: WorkflowResultQueueJobData<unknown>,
    options?: WorkflowJobOptions,
  ) {
    if (!this.failed) {
      this.failed = true;
      return Promise.reject(new Error('result-queue-temporary-failure'));
    }

    return super.add(name, data, options);
  }
}

class DuplicateJobIdResultQueue extends CapturingResultQueue {
  private readonly jobsById = new Map<
    string,
    WorkflowResultQueueJobData<unknown>
  >();

  override add(
    name: string,
    data: WorkflowResultQueueJobData<unknown>,
    options?: WorkflowJobOptions,
  ) {
    const jobId = options?.jobId;
    const normalizedJobId =
      jobId === undefined || jobId === null ? undefined : String(jobId);

    if (normalizedJobId && this.jobsById.has(normalizedJobId)) {
      return Promise.reject(new Error(`Job ${normalizedJobId} already exists`));
    }

    if (normalizedJobId) {
      this.jobsById.set(normalizedJobId, data);
    }

    return super.add(name, data, options);
  }

  override getJob(jobId?: string) {
    if (!jobId) {
      return Promise.resolve(null);
    }

    const data = this.jobsById.get(String(jobId));
    if (!data) {
      return Promise.resolve(null);
    }

    return Promise.resolve({
      id: String(jobId),
      name: 'workflow-result',
      data,
      updateData: (
        nextData: WorkflowResultQueueJobData<unknown>,
      ): Promise<void> => {
        this.jobsById.set(String(jobId), nextData);
        return Promise.resolve();
      },
    });
  }
}

@Injectable()
class OnFailedSpy {
  calls: Array<{ error: Error; input: unknown; jobId: string }> = [];
  throwOnCall = false;
}

@Workflow({ name: 'on-failed-workflow' })
class OnFailedWorkflow {
  constructor(private readonly spy: OnFailedSpy) {}

  @Step({ name: 'fail-step' })
  failStep(): Promise<void> {
    throw new Error('step-on-failed-error');
  }

  async run(input: { value: number }): Promise<void> {
    await this.failStep();
  }

  async onFailed(
    error: Error,
    input: { value: number },
    jobId: string,
  ): Promise<void> {
    if (this.spy.throwOnCall) {
      throw new Error('on-failed-handler-threw');
    }
    this.spy.calls.push({ error, input, jobId });
  }
}

@Workflow({ name: 'on-failed-non-retryable-workflow' })
class OnFailedNonRetryableWorkflow {
  constructor(private readonly spy: OnFailedSpy) {}

  @Step({ name: 'non-retryable-step' })
  failStep(): Promise<void> {
    throw new NonRetryableError('non-retryable-step-error');
  }

  async run(): Promise<void> {
    await this.failStep();
  }

  async onFailed(error: Error, input: unknown, jobId: string): Promise<void> {
    this.spy.calls.push({ error, input, jobId });
  }
}

@Workflow({ name: 'no-on-failed-workflow' })
class NoOnFailedWorkflow {
  @Step({ name: 'fail' })
  fail(): Promise<void> {
    throw new Error('no-handler-error');
  }

  async run(): Promise<void> {
    await this.fail();
  }
}

@Workflow({ name: 'recovery-workflow' })
class RecoveryWorkflow {
  constructor(private readonly stats: RecoveryStats) {}

  @Step({ name: 'validate' })
  validate(input: RecoveryPayload): Promise<RecoveryPayload> {
    this.stats.validate += 1;
    return Promise.resolve({ ...input, validated: true });
  }

  @Step({ name: 'process' })
  process(input: RecoveryPayload): Promise<RecoveryPayload> {
    this.stats.process += 1;
    if (this.stats.process === 1) {
      throw new Error('transient-process-failure');
    }

    return Promise.resolve({ ...input, processed: true });
  }

  @Step({ name: 'store' })
  store(input: RecoveryPayload): Promise<RecoveryPayload> {
    this.stats.store += 1;
    return Promise.resolve(input);
  }

  async run(
    input: RecoveryPayload,
  ): Promise<{ success: true; payload: RecoveryPayload }> {
    const validated = await this.validate(input);
    const processed = await this.process(validated);
    await this.store(processed);
    return { success: true, payload: processed };
  }
}

@Workflow({ name: 'retry-workflow' })
class RetryWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  @Step({ name: 'unstable', retry: { attempts: 3 } })
  unstable(value: number): Promise<number> {
    if (this.failOnce.shouldFail('retry-workflow', 2)) {
      throw new Error('temporary-error');
    }

    return Promise.resolve(value + 1);
  }

  run(input: { value: number }): Promise<number> {
    return this.unstable(input.value);
  }
}

@Workflow({ name: 'typed-step-workflow' })
class TypedStepWorkflow {
  @Step({ name: 'void-step' })
  doNothing(): Promise<void> {
    return Promise.resolve();
  }

  @Step({ name: 'undefined-step' })
  returnUndefined(): Promise<undefined> {
    return Promise.resolve(undefined);
  }

  @Step({ name: 'value-step' })
  plusOne(value: number): Promise<number> {
    return Promise.resolve(value + 1);
  }

  async run(input: { value: number }): Promise<number> {
    await this.doNothing();
    await this.returnUndefined();
    return this.plusOne(input.value);
  }
}

@Workflow({ name: 'repeated-step-workflow' })
class RepeatedStepWorkflow {
  @Step({ name: 'inc' })
  inc(value: number): Promise<number> {
    return Promise.resolve(value + 1);
  }

  async run(input: { value: number }): Promise<number> {
    const first = await this.inc(input.value);
    return this.inc(first);
  }
}

@Workflow({ name: 'typed-input-workflow' })
class TypedInputWorkflow {
  @Step({ name: 'normalize' })
  normalize(
    input:
      | { kind: 'number'; value: number }
      | { kind: 'string'; value: string }
      | { kind: 'array'; value: string[] },
  ): Promise<string> {
    switch (input.kind) {
      case 'number':
        return Promise.resolve(String(input.value));
      case 'string':
        return Promise.resolve(input.value);
      case 'array':
        return Promise.resolve(input.value.join(','));
    }
  }

  async run(
    input:
      | { kind: 'number'; value: number }
      | { kind: 'string'; value: string }
      | { kind: 'array'; value: string[] },
  ): Promise<{ normalized: string }> {
    return { normalized: await this.normalize(input) };
  }
}

@Workflow({ name: 'nondeterministic-workflow' })
class NonDeterministicWorkflow {
  constructor(
    private readonly branch: BranchService,
    private readonly failOnce: FailOnceService,
  ) {}

  @Step({ name: 'left-branch' })
  left(input: number): Promise<number> {
    return Promise.resolve(input + 10);
  }

  @Step({ name: 'right-branch' })
  right(input: number): Promise<number> {
    return Promise.resolve(input + 20);
  }

  @Step({ name: 'fail-once' })
  fail(input: string): Promise<void> {
    if (this.failOnce.shouldFail(input)) {
      throw new Error('fail-once');
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const value =
      this.branch.branch === 'left'
        ? await this.left(input.value)
        : await this.right(input.value);

    await this.fail(input.id);
    return { value };
  }
}

@Workflow({ name: 'binary-input-workflow' })
class BinaryInputWorkflow {
  constructor(
    private readonly stats: BinaryStats,
    private readonly failOnce: FailOnceService,
  ) {}

  @Step({ name: 'inspect' })
  inspect(input: {
    id: string;
    bytes: Uint8Array;
    arrayBuffer: ArrayBuffer;
    buffer: Buffer;
    blob?: Blob;
  }): Promise<{
    isUint8Array: boolean;
    isArrayBuffer: boolean;
    isBuffer: boolean;
    blobSize: number;
    bytesSum: number;
  }> {
    this.stats.inspected += 1;

    const bytesSum = input.bytes.reduce((acc, current) => acc + current, 0);
    return Promise.resolve({
      isUint8Array: input.bytes instanceof Uint8Array,
      isArrayBuffer: input.arrayBuffer instanceof ArrayBuffer,
      isBuffer: Buffer.isBuffer(input.buffer),
      blobSize: input.blob?.size ?? 0,
      bytesSum,
    });
  }

  @Step({ name: 'fail-once' })
  fail(id: string): Promise<void> {
    if (this.failOnce.shouldFail(`binary-input:${id}`)) {
      throw new Error('binary-input-fail-once');
    }

    return Promise.resolve();
  }

  async run(input: {
    id: string;
    bytes: Uint8Array;
    arrayBuffer: ArrayBuffer;
    buffer: Buffer;
    blob?: Blob;
  }): Promise<{
    isUint8Array: boolean;
    isArrayBuffer: boolean;
    isBuffer: boolean;
    blobSize: number;
    bytesSum: number;
  }> {
    const inspected = await this.inspect(input);
    await this.fail(input.id);
    return inspected;
  }
}

@Workflow({ name: 'typed-array-result-workflow' })
class TypedArrayResultWorkflow {
  constructor(
    private readonly stats: BinaryStats,
    private readonly failOnce: FailOnceService,
  ) {}

  @Step({ name: 'produce' })
  produce(): Promise<Uint16Array> {
    this.stats.produced += 1;
    return Promise.resolve(new Uint16Array([1000, 2000]));
  }

  @Step({ name: 'fail-once' })
  fail(id: string): Promise<void> {
    if (this.failOnce.shouldFail(`typed-array-result:${id}`)) {
      throw new Error('typed-array-result-fail-once');
    }

    return Promise.resolve();
  }

  @Step({ name: 'consume' })
  consume(
    payload: Uint16Array,
  ): Promise<{ sum: number; isTypedArray: boolean }> {
    return Promise.resolve({
      sum: payload[0] + payload[1],
      isTypedArray: payload instanceof Uint16Array,
    });
  }

  async run(input: {
    id: string;
  }): Promise<{ sum: number; isTypedArray: boolean }> {
    const payload = await this.produce();
    await this.fail(input.id);
    return this.consume(payload);
  }
}

@Workflow({ name: 'non-serializable-step-result-workflow' })
class NonSerializableStepResultWorkflow {
  @Step({ name: 'bad-result' })
  badResult(): Promise<{ fn: () => number }> {
    return Promise.resolve({
      fn: () => 42,
    });
  }

  run(): Promise<{ fn: () => number }> {
    return this.badResult();
  }
}

@Workflow({ name: 'date-payload-workflow' })
class DatePayloadWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  @Step({ name: 'normalize-date' })
  normalizeDate(input: {
    id: string;
    at: Date;
  }): Promise<{ iso: string; isDate: boolean }> {
    return Promise.resolve({
      iso: input.at.toISOString(),
      isDate: input.at instanceof Date,
    });
  }

  @Step({ name: 'fail-once' })
  fail(id: string): Promise<void> {
    if (this.failOnce.shouldFail(`date-payload:${id}`)) {
      throw new Error('date-payload-fail-once');
    }

    return Promise.resolve();
  }

  async run(input: {
    id: string;
    at: Date;
  }): Promise<{ iso: string; isDate: boolean }> {
    const normalized = await this.normalizeDate(input);
    await this.fail(input.id);
    return normalized;
  }
}

@Workflow({ name: 'date-step-result-workflow' })
class DateStepResultWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  @Step({ name: 'make-date' })
  makeDate(input: { year: number }): Promise<Date> {
    return Promise.resolve(new Date(Date.UTC(input.year, 0, 2, 3, 4, 5, 0)));
  }

  @Step({ name: 'fail-once' })
  fail(id: string): Promise<void> {
    if (this.failOnce.shouldFail(`date-result:${id}`)) {
      throw new Error('date-result-fail-once');
    }

    return Promise.resolve();
  }

  @Step({ name: 'consume-date' })
  consumeDate(value: Date): Promise<{ iso: string; isDate: boolean }> {
    return Promise.resolve({
      iso: value.toISOString(),
      isDate: value instanceof Date,
    });
  }

  async run(input: {
    id: string;
    year: number;
  }): Promise<{ iso: string; isDate: boolean }> {
    const created = await this.makeDate({ year: input.year });
    await this.fail(input.id);
    return this.consumeDate(created);
  }
}

@Workflow({ name: 'nested-replay-workflow' })
class NestedReplayWorkflow {
  constructor(
    private readonly stats: NestedReplayStats,
    private readonly failOnce: FailOnceService,
  ) {}

  @Step({ name: 'outer' })
  outer(input: { value: number }): Promise<number> {
    this.stats.outer += 1;
    return this.inner(input.value);
  }

  @Step({ name: 'inner' })
  inner(value: number): Promise<number> {
    this.stats.inner += 1;
    return Promise.resolve(value + 1);
  }

  @Step({ name: 'fail-once' })
  fail(id: string): Promise<void> {
    this.stats.fail += 1;
    if (this.failOnce.shouldFail(`nested-replay:${id}`)) {
      throw new Error('nested-replay-fail-once');
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const value = await this.outer({ value: input.value });
    await this.fail(input.id);
    return { value };
  }
}

@Workflow({
  name: 'workflow-default-retry-workflow',
  execution: {
    stepRetry: {
      attempts: 2,
    },
  },
})
class WorkflowDefaultRetryWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  @Step({ name: 'unstable' })
  unstable(input: { id: string; value: number }): Promise<number> {
    if (this.failOnce.shouldFail(`workflow-default-retry:${input.id}`)) {
      throw new Error('workflow-default-retry-fail-once');
    }

    return Promise.resolve(input.value + 1);
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.unstable(input);
  }
}

@Workflow({ name: 'global-default-retry-workflow' })
class GlobalDefaultRetryWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  @Step({ name: 'unstable' })
  unstable(input: { id: string; value: number }): Promise<number> {
    if (this.failOnce.shouldFail(`global-default-retry:${input.id}`)) {
      throw new Error('global-default-retry-fail-once');
    }

    return Promise.resolve(input.value + 1);
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.unstable(input);
  }
}

@Workflow({ name: 'global-default-retry-override-workflow' })
class GlobalDefaultRetryOverrideWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  @Step({
    name: 'unstable',
    retry: {
      attempts: 1,
    },
  })
  unstable(input: { id: string; value: number }): Promise<number> {
    if (this.failOnce.shouldFail(`global-default-retry-override:${input.id}`)) {
      throw new Error('global-default-retry-override-fail-once');
    }

    return Promise.resolve(input.value + 1);
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.unstable(input);
  }
}

@Workflow({ name: 'retry-restarts-whole-flow-workflow' })
class RetryRestartsWholeFlowWorkflow {
  private localCounter = 0;

  constructor(private readonly failOnce: FailOnceService) {}

  @Step({ name: 'mutating-step', retry: { attempts: 2 } })
  mutatingStep(input: { id: string; value: number }): Promise<number> {
    this.localCounter += 1;

    if (this.failOnce.shouldFail(`retry-restart:${input.id}`)) {
      throw new Error('retry-restart-fail-once');
    }

    if (this.localCounter !== 1) {
      throw new Error('workflow-local-state-corrupted');
    }

    return Promise.resolve(input.value + 1);
  }

  run(input: { id: string; value: number }): Promise<number> {
    return this.mutatingStep(input);
  }
}

@Workflow({
  name: 'job-options-workflow',
  job: {
    attempts: 2,
    removeOnComplete: true,
  },
})
class JobOptionsWorkflow {
  run(input: { value: number }): Promise<number> {
    return Promise.resolve(input.value);
  }
}

@Workflow({
  name: 'result-queue-workflow',
  resultQueue: {
    jobName: 'workflow-result',
    job: {
      removeOnComplete: true,
    },
  },
})
class ResultQueueWorkflow {
  run(input: { value: number }): Promise<{ value: number }> {
    return Promise.resolve({ value: input.value + 1 });
  }
}

@Workflow({
  name: 'determinism-probe-stable-workflow',
  execution: {
    autoDeterminismProbe: true,
    determinismProbeMaxDurationMs: 30,
  },
})
class DeterminismProbeStableWorkflow {
  constructor(private readonly stats: DeterminismProbeStats) {}

  @Step({ name: 'compute' })
  compute(input: { value: number }): Promise<{ value: number }> {
    this.stats.computeCalls += 1;
    return Promise.resolve({ value: input.value + 1 });
  }

  run(input: { value: number }): Promise<{ value: number }> {
    return this.compute(input);
  }
}

@Workflow({
  name: 'determinism-probe-random-workflow',
  execution: {
    autoDeterminismProbe: true,
    determinismProbeMaxDurationMs: 30,
  },
})
class DeterminismProbeRandomWorkflow {
  run(): Promise<{ value: number }> {
    return Promise.resolve({ value: Math.random() });
  }
}

@Workflow({
  name: 'determinism-probe-slow-workflow',
  execution: {
    autoDeterminismProbe: true,
    determinismProbeMaxDurationMs: 1,
  },
})
class DeterminismProbeSlowWorkflow {
  async run(input: { value: number }): Promise<{ value: number }> {
    await sleep(5);
    return { value: input.value + 1 };
  }
}

@Workflow({ name: 'global-determinism-probe-random-workflow' })
class GlobalDeterminismProbeRandomWorkflow {
  run(): Promise<{ value: number }> {
    return Promise.resolve({ value: Math.random() });
  }
}

@Workflow({ name: 'global-workflow-retry-workflow' })
class GlobalWorkflowRetryWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  run(input: { id: string; value: number }): Promise<{ value: number }> {
    if (this.failOnce.shouldFail(`global-workflow-retry:${input.id}`, 1)) {
      return Promise.reject(new Error('global-workflow-retry-fail-once'));
    }

    return Promise.resolve({ value: input.value + 1 });
  }
}

@Workflow({
  name: 'global-workflow-retry-override-workflow',
  execution: {
    workflowRetry: {
      attempts: 1,
    },
  },
})
class GlobalWorkflowRetryOverrideWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  run(input: { id: string; value: number }): Promise<{ value: number }> {
    if (
      this.failOnce.shouldFail(`global-workflow-retry-override:${input.id}`, 1)
    ) {
      return Promise.reject(
        new Error('global-workflow-retry-override-fail-once'),
      );
    }

    return Promise.resolve({ value: input.value + 1 });
  }
}

@Workflow({ name: 'non-retryable-step-workflow' })
class NonRetryableStepWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  @Step({
    name: 'validate-minimum',
    retry: {
      attempts: 5,
    },
  })
  validateMinimum(input: { id: string; amount: number }): Promise<boolean> {
    if (this.failOnce.shouldFail(`non-retryable:${input.id}`, 1)) {
      throw new NonRetryableError('Amount below minimum');
    }

    return Promise.resolve(input.amount >= 100);
  }

  run(input: { id: string; amount: number }): Promise<boolean> {
    return this.validateMinimum(input);
  }
}

@Workflow({ name: 'timeout-compensation-workflow' })
class TimeoutCompensationWorkflow {
  constructor(private readonly stats: TimeoutCompensationStats) {}

  @Step({
    name: 'process-order',
    timeout: 5,
  })
  async processOrder(): Promise<void> {
    this.stats.timedOut += 1;
    await sleep(20);
  }

  @Step({ name: 'cancel-order' })
  cancelOrder(): Promise<void> {
    this.stats.cleanup += 1;
    return Promise.resolve();
  }

  async run(): Promise<{ compensated: boolean }> {
    try {
      await this.processOrder();
      return { compensated: false };
    } catch (error) {
      if (error instanceof TimeoutError) {
        await this.cancelOrder();
        return { compensated: true };
      }
      throw error;
    }
  }
}

@Workflow({
  name: 'workflow-auto-resume-workflow',
  execution: {
    workflowRetry: {
      attempts: 2,
      delayMs: 1,
      strategy: 'constant',
    },
  },
})
class WorkflowAutoResumeWorkflow {
  constructor(
    private readonly failOnce: FailOnceService,
    private readonly stats: WorkflowAutoResumeStats,
  ) {}

  @Step({ name: 'prepare' })
  prepare(value: number): Promise<number> {
    this.stats.prepare += 1;
    return Promise.resolve(value + 1);
  }

  unstable(id: string): Promise<void> {
    this.stats.unstable += 1;
    if (this.failOnce.shouldFail(`workflow-auto-resume:${id}`, 1)) {
      return Promise.reject(new Error('workflow-auto-resume-fail-once'));
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const prepared = await this.prepare(input.value);
    await this.unstable(input.id);
    return { value: prepared };
  }
}

@Workflow({
  name: 'workflow-retry-linear-workflow',
  execution: {
    workflowRetry: {
      attempts: 3,
      delayMs: 2,
      strategy: 'linear',
    },
  },
})
class WorkflowRetryLinearWorkflow {
  constructor(private readonly failOnce: FailOnceService) {}

  run(input: { id: string; value: number }): Promise<{ value: number }> {
    if (this.failOnce.shouldFail(`workflow-retry-linear:${input.id}`, 2)) {
      return Promise.reject(new Error('workflow-retry-linear-fail'));
    }

    return Promise.resolve({ value: input.value + 1 });
  }
}

@Workflow({ name: 'duplicate-workflow-name' })
class DuplicateNameWorkflowA {
  run(): Promise<{ ok: true }> {
    return Promise.resolve({ ok: true });
  }
}

@Workflow({ name: 'duplicate-workflow-name' })
class DuplicateNameWorkflowB {
  run(): Promise<{ ok: true }> {
    return Promise.resolve({ ok: true });
  }
}

describe('DozerEngine (library unit tests)', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();

    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: queue,
        }),
        DozerModule.forFeature(
          [
            RecoveryWorkflow,
            RetryWorkflow,
            TypedStepWorkflow,
            RepeatedStepWorkflow,
            TypedInputWorkflow,
            NonDeterministicWorkflow,
            BinaryInputWorkflow,
            TypedArrayResultWorkflow,
            NonSerializableStepResultWorkflow,
            DatePayloadWorkflow,
            DateStepResultWorkflow,
            NestedReplayWorkflow,
            WorkflowDefaultRetryWorkflow,
            GlobalDefaultRetryWorkflow,
            GlobalDefaultRetryOverrideWorkflow,
            RetryRestartsWholeFlowWorkflow,
            JobOptionsWorkflow,
            DeterminismProbeStableWorkflow,
            DeterminismProbeRandomWorkflow,
            DeterminismProbeSlowWorkflow,
            GlobalDeterminismProbeRandomWorkflow,
            NonRetryableStepWorkflow,
            TimeoutCompensationWorkflow,
            WorkflowAutoResumeWorkflow,
            WorkflowRetryLinearWorkflow,
          ],
          [
            RecoveryStats,
            BranchService,
            FailOnceService,
            BinaryStats,
            NestedReplayStats,
            DeterminismProbeStats,
            TimeoutCompensationStats,
            WorkflowAutoResumeStats,
          ],
        ),
      ],
    }).compile();

    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
  });

  afterEach(async () => {
    if (moduleRef) {
      await moduleRef.close();
    }
  });

  it('restores workflow state and replays completed steps only once', async () => {
    const stats = moduleRef.get(RecoveryStats);

    const jobId = await engine.start('recovery-workflow', { orderId: 42 });

    await expect(engine.run(jobId)).rejects.toThrow(
      'transient-process-failure',
    );

    const failedJob = await queue.get(jobId);
    const failedState = failedJob?.data[DOZER_JOB_STATE_KEY];

    expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
    expect(failedState?.c['0:validate']).toBeDefined();
    expect(failedState?.c['1:process']).toBeUndefined();
    expect(stats.validate).toBe(1);
    expect(stats.process).toBe(1);

    const result = await engine.run(jobId);
    expect(result).toEqual({
      success: true,
      payload: { orderId: 42, validated: true, processed: true },
    });

    const completedJob = await queue.get(jobId);
    const completedState = completedJob?.data[DOZER_JOB_STATE_KEY];
    expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
    expect(stats.validate).toBe(1);
    expect(stats.process).toBe(2);
    expect(stats.store).toBe(1);
  });

  it('retries unstable steps by retry policy', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('retry-workflow', { value: 1 });
    const result = await engine.run(jobId);
    expect(result).toBe(2);
    expect(failOnce.calls('retry-workflow')).toBe(3);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
  });

  it('returns workflow job info with status and result by jobId', async () => {
    const jobId = await engine.start('typed-input-workflow', {
      kind: 'number',
      value: 42,
    });

    await expect(engine.getJobInfo(jobId)).resolves.toMatchObject({
      id: jobId,
      name: 'typed-input-workflow',
      status: WORKFLOW_STATUS.pending,
      statusName: 'pending',
      result: undefined,
    });

    await expect(engine.run(jobId)).resolves.toEqual({ normalized: '42' });

    await expect(engine.getJobInfo(jobId)).resolves.toMatchObject({
      id: jobId,
      name: 'typed-input-workflow',
      status: WORKFLOW_STATUS.completed,
      statusName: 'completed',
      result: { normalized: '42' },
    });
  });

  it('cancels pending workflow job and prevents running it', async () => {
    const jobId = await engine.start('retry-workflow', { value: 1 });

    await expect(engine.cancel(jobId)).resolves.toBe(true);
    await expect(engine.cancel(jobId)).resolves.toBe(false);
    await expect(engine.getJobInfo(jobId)).resolves.toMatchObject({
      id: jobId,
      status: WORKFLOW_STATUS.cancelled,
      statusName: 'cancelled',
    });
    await expect(engine.run(jobId)).rejects.toBeInstanceOf(
      WorkflowCancelledError,
    );
  });

  it('does not retry step when NonRetryableError is thrown', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('non-retryable-step-workflow', {
      id: 'non-retryable-1',
      amount: 10,
    });

    await expect(engine.run(jobId)).rejects.toBeInstanceOf(NonRetryableError);
    expect(failOnce.calls('non-retryable:non-retryable-1')).toBe(1);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.failed);
  });

  it('supports timeout handling with compensating actions in workflow run', async () => {
    const stats = moduleRef.get(TimeoutCompensationStats);
    stats.timedOut = 0;
    stats.cleanup = 0;

    const jobId = await engine.start('timeout-compensation-workflow', {});
    await expect(engine.run(jobId)).resolves.toEqual({ compensated: true });

    expect(stats.timedOut).toBe(1);
    expect(stats.cleanup).toBe(1);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.u?.['0:process-order']).toBe(
      undefined,
    );
    expect(job?.data[DOZER_JOB_STATE_KEY]?.u?.['1:cancel-order']).toBe(1);
  });

  it('automatically resumes workflow by workflowRetry settings', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    const stats = moduleRef.get(WorkflowAutoResumeStats);
    failOnce.reset();
    stats.prepare = 0;
    stats.unstable = 0;

    const jobId = await engine.start('workflow-auto-resume-workflow', {
      id: 'workflow-auto-resume-1',
      value: 5,
    });

    await expect(engine.run(jobId)).resolves.toEqual({ value: 6 });
    expect(stats.prepare).toBe(1);
    expect(stats.unstable).toBe(2);
    expect(failOnce.calls('workflow-auto-resume:workflow-auto-resume-1')).toBe(
      2,
    );

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
  });

  it('applies workflow retry backoff strategy delays', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('workflow-retry-linear-workflow', {
      id: 'workflow-retry-linear-1',
      value: 4,
    });

    const startedAt = Date.now();
    await expect(engine.run(jobId)).resolves.toEqual({ value: 5 });
    const elapsedMs = Date.now() - startedAt;

    expect(elapsedMs).toBeGreaterThanOrEqual(5);
    expect(
      failOnce.calls('workflow-retry-linear:workflow-retry-linear-1'),
    ).toBe(3);
  });

  it('applies workflow-level default retry options for steps without own retry', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('workflow-default-retry-workflow', {
      id: 'workflow-default-retry-1',
      value: 3,
    });
    await expect(engine.run(jobId)).resolves.toBe(4);
    expect(
      failOnce.calls('workflow-default-retry:workflow-default-retry-1'),
    ).toBe(2);
  });

  it('restarts whole workflow on step retry using a fresh workflow instance', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('retry-restarts-whole-flow-workflow', {
      id: 'retry-restart-1',
      value: 10,
    });
    await expect(engine.run(jobId)).resolves.toBe(11);
    expect(failOnce.calls('retry-restart:retry-restart-1')).toBe(2);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
  });

  it('supports steps that return void and undefined', async () => {
    const jobId = await engine.start('typed-step-workflow', { value: 2 });
    const result = await engine.run(jobId);

    expect(result).toBe(3);

    const job = await queue.get(jobId);
    const state = job?.data[DOZER_JOB_STATE_KEY];

    expect(state?.u?.['0:void-step']).toBe(1);
    expect(state?.u?.['1:undefined-step']).toBe(1);
    expect(state?.c['2:value-step']).toBe(3);
  });

  it('handles repeated calls of the same step method as separate step keys', async () => {
    const jobId = await engine.start('repeated-step-workflow', { value: 1 });
    const result = await engine.run(jobId);

    expect(result).toBe(3);

    const job = await queue.get(jobId);
    const state = job?.data[DOZER_JOB_STATE_KEY];
    expect(state?.c['0:inc']).toBe(2);
    expect(state?.c['1:inc']).toBe(3);
  });

  it('supports workflows with different input data types', async () => {
    const numberJob = await engine.start('typed-input-workflow', {
      kind: 'number',
      value: 5,
    });
    const stringJob = await engine.start('typed-input-workflow', {
      kind: 'string',
      value: 'abc',
    });
    const arrayJob = await engine.start('typed-input-workflow', {
      kind: 'array',
      value: ['a', 'b'],
    });

    await expect(engine.run(numberJob)).resolves.toEqual({ normalized: '5' });
    await expect(engine.run(stringJob)).resolves.toEqual({ normalized: 'abc' });
    await expect(engine.run(arrayJob)).resolves.toEqual({ normalized: 'a,b' });
  });

  it('detects non-deterministic replay', async () => {
    const branch = moduleRef.get(BranchService);
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();
    branch.branch = 'left';

    const jobId = await engine.start('nondeterministic-workflow', {
      id: 'x',
      value: 1,
    });

    await expect(engine.run(jobId)).rejects.toThrow('fail-once');

    branch.branch = 'right';

    await expect(engine.run(jobId)).rejects.toBeInstanceOf(
      StepReplayConflictError,
    );
  });

  it('replays cached nested steps without trace conflicts', async () => {
    const stats = moduleRef.get(NestedReplayStats);
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('nested-replay-workflow', {
      id: 'nested-1',
      value: 5,
    });

    await expect(engine.run(jobId)).rejects.toThrow('nested-replay-fail-once');

    const failedJob = await queue.get(jobId);
    const failedState = failedJob?.data[DOZER_JOB_STATE_KEY];
    expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
    expect(failedState?.c['0:outer']).toBe(6);
    expect(failedState?.c['0.0:inner']).toBeUndefined();
    expect(stats.outer).toBe(1);
    expect(stats.inner).toBe(1);
    expect(stats.fail).toBe(1);

    await expect(engine.run(jobId)).resolves.toEqual({ value: 6 });

    const completedJob = await queue.get(jobId);
    const completedState = completedJob?.data[DOZER_JOB_STATE_KEY];
    expect(completedState?.s).toBe(WORKFLOW_STATUS.completed);
    expect(completedState?.c['0:outer']).toBe(6);
    expect(completedState?.c['0.0:inner']).toBeUndefined();
    expect(completedState?.t).toContain('0.0:inner');
    expect(stats.outer).toBe(1);
    expect(stats.inner).toBe(1);
    expect(stats.fail).toBe(2);
  });

  it('marks state as failed when workflow is not registered', async () => {
    const job = await queue.add('unknown-workflow', {
      [DOZER_JOB_INPUT_KEY]: { any: 'value' },
      [DOZER_JOB_STATE_KEY]: {
        s: WORKFLOW_STATUS.pending,
        c: {},
        t: [],
      },
    });

    await expect(engine.run(job.id)).rejects.toThrow('not registered');

    const failedJob = await queue.get(job.id);
    const failedState = failedJob?.data[DOZER_JOB_STATE_KEY];
    expect(failedState?.s).toBe(WORKFLOW_STATUS.failed);
    expect(String(failedState?.e ?? '')).toContain('not registered');
  });

  it('applies module-level default retry options for steps', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            execution: {
              stepRetry: {
                attempts: 2,
              },
            },
          },
        }),
        DozerModule.forFeature([GlobalDefaultRetryWorkflow], [FailOnceService]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const failOnce = localModule.get(FailOnceService);
      failOnce.reset();

      const jobId = await localEngine.start('global-default-retry-workflow', {
        id: 'global-default-retry-1',
        value: 5,
      });
      await expect(localEngine.run(jobId)).resolves.toBe(6);
      expect(
        failOnce.calls('global-default-retry:global-default-retry-1'),
      ).toBe(2);
    } finally {
      await localModule.close();
    }
  });

  it('lets step-level retry options override module defaults', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            execution: {
              stepRetry: {
                attempts: 3,
              },
            },
          },
        }),
        DozerModule.forFeature(
          [GlobalDefaultRetryOverrideWorkflow],
          [FailOnceService],
        ),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const failOnce = localModule.get(FailOnceService);
      failOnce.reset();

      const jobId = await localEngine.start(
        'global-default-retry-override-workflow',
        {
          id: 'global-default-retry-override-1',
          value: 5,
        },
      );

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'global-default-retry-override-fail-once',
      );
      expect(
        failOnce.calls(
          'global-default-retry-override:global-default-retry-override-1',
        ),
      ).toBe(1);

      await expect(localEngine.run(jobId)).resolves.toBe(6);
      expect(
        failOnce.calls(
          'global-default-retry-override:global-default-retry-override-1',
        ),
      ).toBe(2);
    } finally {
      await localModule.close();
    }
  });

  it('applies module-level default workflowRetry options', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            execution: {
              workflowRetry: {
                attempts: 2,
                delayMs: 1,
                strategy: 'constant',
              },
            },
          },
        }),
        DozerModule.forFeature(
          [GlobalWorkflowRetryWorkflow],
          [FailOnceService],
        ),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const failOnce = localModule.get(FailOnceService);
      failOnce.reset();

      const jobId = await localEngine.start('global-workflow-retry-workflow', {
        id: 'global-workflow-retry-1',
        value: 3,
      });
      await expect(localEngine.run(jobId)).resolves.toEqual({ value: 4 });
      expect(
        failOnce.calls('global-workflow-retry:global-workflow-retry-1'),
      ).toBe(2);
    } finally {
      await localModule.close();
    }
  });

  it('lets workflow-level workflowRetry options override module defaults', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            execution: {
              workflowRetry: {
                attempts: 3,
              },
            },
          },
        }),
        DozerModule.forFeature(
          [GlobalWorkflowRetryOverrideWorkflow],
          [FailOnceService],
        ),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const failOnce = localModule.get(FailOnceService);
      failOnce.reset();

      const jobId = await localEngine.start(
        'global-workflow-retry-override-workflow',
        {
          id: 'global-workflow-retry-override-1',
          value: 3,
        },
      );

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'global-workflow-retry-override-fail-once',
      );
      expect(
        failOnce.calls(
          'global-workflow-retry-override:global-workflow-retry-override-1',
        ),
      ).toBe(1);

      await expect(localEngine.run(jobId)).resolves.toEqual({ value: 4 });
      expect(
        failOnce.calls(
          'global-workflow-retry-override:global-workflow-retry-override-1',
        ),
      ).toBe(2);
    } finally {
      await localModule.close();
    }
  });

  it('merges global and workflow job options when creating jobs', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            job: {
              attempts: 5,
              removeOnFail: 100,
            },
          },
        }),
        DozerModule.forFeature([JobOptionsWorkflow]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('job-options-workflow', {
        value: 10,
      });
      const job = await localQueue.get(jobId);

      expect(job?.options).toEqual({
        attempts: 2,
        removeOnFail: 100,
        removeOnComplete: true,
      });

      const unknownJobId = await localEngine.start('unknown-workflow', {
        value: 10,
      });
      const unknownJob = await localQueue.get(unknownJobId);
      expect(unknownJob?.options).toEqual({
        attempts: 5,
        removeOnFail: 100,
      });
    } finally {
      await localModule.close();
    }
  });

  it('publishes completed workflow result to configured result queue', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const resultQueue = new CapturingResultQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          resultQueue,
        }),
        DozerModule.forFeature([ResultQueueWorkflow]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('result-queue-workflow', {
        value: 10,
      });

      await expect(localEngine.run(jobId)).resolves.toEqual({ value: 11 });

      expect(resultQueue.added).toHaveLength(1);
      expect(resultQueue.added[0]).toMatchObject({
        name: 'workflow-result',
        data: {
          jobId,
          workflowName: 'result-queue-workflow',
          status: 'completed',
          result: { value: 11 },
        },
        options: {
          jobId,
          removeOnComplete: true,
        },
      });
    } finally {
      await localModule.close();
    }
  });

  it('keeps workflow in completing status when result queue publish fails and resumes finalize later', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const resultQueue = new FailOnceResultQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          resultQueue,
        }),
        DozerModule.forFeature([ResultQueueWorkflow]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('result-queue-workflow', {
        value: 20,
      });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'result-queue-temporary-failure',
      );
      await expect(localEngine.getJobInfo(jobId)).resolves.toMatchObject({
        id: jobId,
        status: WORKFLOW_STATUS.completing,
        statusName: 'completing',
        result: { value: 21 },
      });

      await expect(localEngine.run(jobId)).resolves.toEqual({ value: 21 });
      await expect(localEngine.getJobInfo(jobId)).resolves.toMatchObject({
        id: jobId,
        status: WORKFLOW_STATUS.completed,
        statusName: 'completed',
        result: { value: 21 },
      });
      expect(resultQueue.added).toHaveLength(1);
      expect(resultQueue.added[0]?.options).toMatchObject({ jobId });
    } finally {
      await localModule.close();
    }
  });

  it('resumes completing workflow when result job already exists without creating duplicate', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const resultQueue = new DuplicateJobIdResultQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          resultQueue,
        }),
        DozerModule.forFeature([ResultQueueWorkflow]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('result-queue-workflow', {
        value: 30,
      });
      const persistedResult = { value: 31 };

      await resultQueue.add(
        'workflow-result',
        {
          jobId,
          workflowName: 'result-queue-workflow',
          result: persistedResult,
          status: 'completed',
        },
        { jobId },
      );

      const workflowJob = await localQueue.get(jobId);
      await workflowJob?.updateData({
        ...workflowJob?.data,
        [DOZER_JOB_STATE_KEY]: {
          s: WORKFLOW_STATUS.completing,
          c: {},
          a: {},
          t: [],
          r: persistedResult,
        },
      });

      await expect(localEngine.run(jobId)).resolves.toEqual(persistedResult);
      await expect(localEngine.getJobInfo(jobId)).resolves.toMatchObject({
        id: jobId,
        status: WORKFLOW_STATUS.completed,
        statusName: 'completed',
        result: persistedResult,
      });
      expect(resultQueue.added).toHaveLength(1);
      expect(resultQueue.added[0]?.options).toMatchObject({ jobId });
    } finally {
      await localModule.close();
    }
  });

  it('restores binary and byte-array workflow inputs on replay', async () => {
    const stats = moduleRef.get(BinaryStats);
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const bytes = new Uint8Array([1, 2, 3, 4]);
    const arrayBuffer = bytes.buffer.slice(0);
    const buffer = Buffer.from([5, 6, 7]);
    const blob =
      typeof Blob === 'undefined'
        ? undefined
        : new Blob([bytes], { type: 'application/octet-stream' });

    const jobId = await engine.start('binary-input-workflow', {
      id: 'bin-1',
      bytes,
      arrayBuffer,
      buffer,
      blob,
    });

    await expect(engine.run(jobId)).rejects.toThrow('binary-input-fail-once');

    await expect(engine.run(jobId)).resolves.toEqual({
      isUint8Array: true,
      isArrayBuffer: true,
      isBuffer: true,
      blobSize: blob?.size ?? 0,
      bytesSum: 10,
    });
    expect(stats.inspected).toBe(1);
  });

  it('restores typed-array step results on replay', async () => {
    const stats = moduleRef.get(BinaryStats);
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('typed-array-result-workflow', {
      id: 'typed-result-1',
    });

    await expect(engine.run(jobId)).rejects.toThrow(
      'typed-array-result-fail-once',
    );
    await expect(engine.run(jobId)).resolves.toEqual({
      sum: 3000,
      isTypedArray: true,
    });
    expect(stats.produced).toBe(1);
  });

  it('rejects non-serializable workflow input values', async () => {
    await expect(
      engine.start('typed-input-workflow', {
        kind: 'object',
        value: {
          fn: () => 1,
        },
      }),
    ).rejects.toBeInstanceOf(SerializationError);
  });

  it('fails workflow when step result is non-serializable', async () => {
    const jobId = await engine.start('non-serializable-step-result-workflow', {
      id: 1,
    });

    await expect(engine.run(jobId)).rejects.toBeInstanceOf(SerializationError);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.failed);
  });

  it('serializes and restores Date in workflow input', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const at = new Date('2026-01-02T03:04:05.000Z');
    const jobId = await engine.start('date-payload-workflow', {
      id: 'date-input-1',
      at,
    });

    await expect(engine.run(jobId)).rejects.toThrow('date-payload-fail-once');
    await expect(engine.run(jobId)).resolves.toEqual({
      iso: '2026-01-02T03:04:05.000Z',
      isDate: true,
    });
  });

  it('serializes and restores Date step results on replay', async () => {
    const failOnce = moduleRef.get(FailOnceService);
    failOnce.reset();

    const jobId = await engine.start('date-step-result-workflow', {
      id: 'date-result-1',
      year: 2028,
    });

    await expect(engine.run(jobId)).rejects.toThrow('date-result-fail-once');
    await expect(engine.run(jobId)).resolves.toEqual({
      iso: '2028-01-02T03:04:05.000Z',
      isDate: true,
    });
  });

  it('runs determinism probe after completion and reuses cached step results', async () => {
    const stats = moduleRef.get(DeterminismProbeStats);
    stats.computeCalls = 0;

    const jobId = await engine.start('determinism-probe-stable-workflow', {
      value: 1,
    });
    await expect(engine.run(jobId)).resolves.toEqual({ value: 2 });
    expect(stats.computeCalls).toBe(1);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
  });

  it('fails determinism probe when replayed result diverges', async () => {
    const jobId = await engine.start('determinism-probe-random-workflow', {});

    await expect(engine.run(jobId)).rejects.toBeInstanceOf(NonDeterminismError);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.r).toBeDefined();
    expect(job?.data[DOZER_JOB_STATE_KEY]?.e).toBeUndefined();
  });

  it('fails determinism probe when replay run is too slow', async () => {
    const jobId = await engine.start('determinism-probe-slow-workflow', {
      value: 1,
    });

    await expect(engine.run(jobId)).rejects.toBeInstanceOf(NonDeterminismError);

    const job = await queue.get(jobId);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
    expect(job?.data[DOZER_JOB_STATE_KEY]?.r).toBeDefined();
    expect(job?.data[DOZER_JOB_STATE_KEY]?.e).toBeUndefined();
  });

  it('supports module-level defaults for worker determinism probe', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({
          driver: localQueue,
          defaults: {
            execution: {
              autoDeterminismProbe: true,
              determinismProbeMaxDurationMs: 30,
            },
          },
        }),
        DozerModule.forFeature([GlobalDeterminismProbeRandomWorkflow]),
      ],
    }).compile();

    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start(
        'global-determinism-probe-random-workflow',
        {},
      );

      await expect(localEngine.run(jobId)).rejects.toBeInstanceOf(
        NonDeterminismError,
      );
      const job = await localQueue.get(jobId);
      expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.completed);
      expect(job?.data[DOZER_JOB_STATE_KEY]?.r).toBeDefined();
    } finally {
      await localModule.close();
    }
  });

  it('calls onFailed method with error, input, and jobId on terminal failure', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([OnFailedWorkflow], [OnFailedSpy]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedSpy);
      const jobId = await localEngine.start('on-failed-workflow', {
        value: 42,
      });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'step-on-failed-error',
      );

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0].error.message).toBe('step-on-failed-error');
      expect(spy.calls[0].input).toEqual({ value: 42 });
      expect(spy.calls[0].jobId).toBe(jobId);
    } finally {
      await localModule.close();
    }
  });

  it('suppresses errors thrown inside onFailed and still throws original error', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([OnFailedWorkflow], [OnFailedSpy]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedSpy);
      spy.throwOnCall = true;

      const jobId = await localEngine.start('on-failed-workflow', {
        value: 1,
      });

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'step-on-failed-error',
      );
    } finally {
      await localModule.close();
    }
  });

  it('does not crash when workflow has no onFailed method', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature([NoOnFailedWorkflow]),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const jobId = await localEngine.start('no-on-failed-workflow', {});
      await expect(localEngine.run(jobId)).rejects.toThrow('no-handler-error');
    } finally {
      await localModule.close();
    }
  });

  it('calls onFailed when NonRetryableError is thrown', async () => {
    const localQueue = new InMemoryWorkflowQueue();
    const localModule = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: localQueue }),
        DozerModule.forFeature(
          [OnFailedNonRetryableWorkflow],
          [OnFailedSpy],
        ),
      ],
    }).compile();
    await localModule.init();

    try {
      const localEngine = localModule.get(DozerEngine);
      const spy = localModule.get(OnFailedSpy);
      const jobId = await localEngine.start(
        'on-failed-non-retryable-workflow',
        {},
      );

      await expect(localEngine.run(jobId)).rejects.toThrow(
        'non-retryable-step-error',
      );

      expect(spy.calls).toHaveLength(1);
      expect(spy.calls[0].error.message).toBe('non-retryable-step-error');
    } finally {
      await localModule.close();
    }
  });
});

describe('DozerModule registration constraints', () => {
  it('fails when multiple workflows share the same name', async () => {
    const queue = new InMemoryWorkflowQueue();

    await expect(
      Test.createTestingModule({
        imports: [
          DozerModule.forRoot({
            driver: queue,
          }),
          DozerModule.forFeature([
            DuplicateNameWorkflowA,
            DuplicateNameWorkflowB,
          ]),
        ],
      }).compile(),
    ).rejects.toThrow('already registered');
  });
});

describe('DozerClient module', () => {
  it('starts workflows from client-only module without worker providers', async () => {
    const queue = new InMemoryWorkflowQueue();
    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forClient({
          driver: queue,
          defaults: {
            job: {
              attempts: 3,
            },
          },
        }),
      ],
    }).compile();

    await moduleRef.init();

    try {
      const client = moduleRef.get(DozerClient);
      const jobId = await client.start(
        'external-workflow',
        { orderId: 42 },
        { removeOnComplete: true },
      );
      const job = await queue.get(jobId);

      expect(job).toBeDefined();
      expect(job?.name).toBe('external-workflow');
      expect(job?.data[DOZER_JOB_INPUT_KEY]).toEqual({ orderId: 42 });
      expect(job?.data[DOZER_JOB_STATE_KEY]?.s).toBe(WORKFLOW_STATUS.pending);
      expect(job?.options).toEqual({
        attempts: 3,
        removeOnComplete: true,
      });

      await expect(client.getJobInfo(jobId)).resolves.toMatchObject({
        id: jobId,
        name: 'external-workflow',
        status: WORKFLOW_STATUS.pending,
        statusName: 'pending',
      });
      await expect(client.cancel(jobId)).resolves.toBe(true);
      await expect(client.getJobInfo(jobId)).resolves.toMatchObject({
        id: jobId,
        status: WORKFLOW_STATUS.cancelled,
        statusName: 'cancelled',
      });
    } finally {
      await moduleRef.close();
    }
  });

  it('reads and waits for workflow results from configured result queue', async () => {
    const queue = new InMemoryWorkflowQueue();
    const resultQueue = new DuplicateJobIdResultQueue();
    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forClient({
          driver: queue,
          resultQueue,
        }),
      ],
    }).compile();

    await moduleRef.init();

    try {
      const client = moduleRef.get(DozerClient);
      const jobId = await client.start('external-result-workflow', {
        id: 1,
      });

      await expect(client.hasResult(jobId)).resolves.toBe(false);
      await expect(client.getResult(jobId)).resolves.toBeNull();

      const resultQueueJobId = toWorkflowResultQueueJobId(jobId);
      await resultQueue.add(
        'workflow-result',
        {
          jobId,
          workflowName: 'external-result-workflow',
          result: {
            __dozer_serialized__: 'date',
            v: '2026-02-25T00:00:00.000Z',
          },
          status: 'completed',
        },
        { jobId: resultQueueJobId },
      );

      await expect(client.hasResult(jobId)).resolves.toBe(true);
      await expect(client.getResultJob<Date>(jobId)).resolves.toMatchObject({
        id: resultQueueJobId,
        name: 'workflow-result',
        jobId,
        workflowName: 'external-result-workflow',
      });
      await expect(client.getResult<Date>(jobId)).resolves.toBeInstanceOf(Date);
      await expect(client.waitForResult<Date>(jobId)).resolves.toBeInstanceOf(
        Date,
      );
    } finally {
      await moduleRef.close();
    }
  });

  it('fails fast in waitForResult when workflow reaches failed status without result payload', async () => {
    const queue = new InMemoryWorkflowQueue();
    const resultQueue = new DuplicateJobIdResultQueue();
    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forClient({
          driver: queue,
          resultQueue,
        }),
      ],
    }).compile();

    await moduleRef.init();

    try {
      const client = moduleRef.get(DozerClient);
      const jobId = await client.start('external-result-workflow', {
        id: 2,
      });
      const job = await queue.get(jobId);
      await job?.updateData({
        ...job.data,
        [DOZER_JOB_STATE_KEY]: {
          ...(job.data[DOZER_JOB_STATE_KEY] ?? { c: {}, a: {}, t: [] }),
          s: WORKFLOW_STATUS.failed,
          e: 'failed-for-test',
        },
      });

      await expect(
        client.waitForResult(jobId, { timeoutMs: 50, pollMs: 5 }),
      ).rejects.toThrow('finished with status "failed"');
    } finally {
      await moduleRef.close();
    }
  });

  it('decodes result queue job payload and deserializes result for handler wrapper', async () => {
    const rawJob = {
      id: '$123',
      name: 'workflow-result',
      data: {
        jobId: '123',
        workflowName: 'my-workflow',
        status: 'completed',
        result: {
          __dozer_serialized__: 'date',
          v: '2026-02-25T00:00:00.000Z',
        },
      },
    } as unknown as Parameters<typeof decodeWorkflowResultJob<Date>>[0];

    const decoded = decodeWorkflowResultJob<Date>(rawJob);
    expect(decoded).toMatchObject({
      resultJobId: '$123',
      resultJobName: 'workflow-result',
      workflowJobId: '123',
      workflowName: 'my-workflow',
      status: 'completed',
    });
    expect(decoded.result).toBeInstanceOf(Date);

    const handler = jest.fn<Promise<string>, [typeof decoded, typeof rawJob]>(
      (message) => {
        expect(message.result).toBeInstanceOf(Date);
        return Promise.resolve('ok');
      },
    );
    const processor = createWorkflowResultProcessor<Date, string>(handler);
    await expect(processor(rawJob as never)).resolves.toBe('ok');
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('requires real BullMQ Queue instance for createResultWorker convenience method', async () => {
    const queue = new InMemoryWorkflowQueue();
    const resultQueue = new DuplicateJobIdResultQueue();
    const moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forClient({
          driver: queue,
          resultQueue,
        }),
      ],
    }).compile();

    await moduleRef.init();

    try {
      const client = moduleRef.get(DozerClient);
      expect(() => client.createResultWorker(() => undefined)).toThrow(
        'requires a real BullMQ Queue instance',
      );
    } finally {
      await moduleRef.close();
    }
  });
});
