import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  SerializationError,
  Step,
  Workflow,
  WORKFLOW_STATUS,
} from './index';
import { FailOnceService } from './test/workflow-test-utils';
import { DozerWorkflow } from './workflow/dozer-workflow';

@Injectable()
class BinaryStats {
  inspected = 0;
  produced = 0;
}

@Workflow({ name: 'binary-input-workflow' })
class BinaryInputWorkflow extends DozerWorkflow<unknown> {
  constructor(
    private readonly stats: BinaryStats,
    private readonly failOnce: FailOnceService,
  ) {
    super();
  }

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
class TypedArrayResultWorkflow extends DozerWorkflow<{ id: string }> {
  constructor(
    private readonly stats: BinaryStats,
    private readonly failOnce: FailOnceService,
  ) {
    super();
  }

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
class NonSerializableStepResultWorkflow extends DozerWorkflow<unknown> {
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
class DatePayloadWorkflow extends DozerWorkflow<{ id: string; at: Date }> {
  constructor(private readonly failOnce: FailOnceService) {
    super();
  }

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
class DateStepResultWorkflow extends DozerWorkflow<{ id: string; year: number }> {
  constructor(private readonly failOnce: FailOnceService) {
    super();
  }

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

@Workflow({ name: 'simple-serialization-workflow' })
class SimpleSerializationWorkflow extends DozerWorkflow<Record<string, unknown>> {
  run(input: Record<string, unknown>): Promise<Record<string, unknown>> {
    return Promise.resolve(input);
  }
}

describe('DozerEngine serialization', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature(
          [
            BinaryInputWorkflow,
            TypedArrayResultWorkflow,
            NonSerializableStepResultWorkflow,
            DatePayloadWorkflow,
            DateStepResultWorkflow,
            SimpleSerializationWorkflow,
          ],
          [BinaryStats, FailOnceService],
        ),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
  });

  afterEach(async () => {
    if (moduleRef) await moduleRef.close();
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
      engine.start('simple-serialization-workflow', {
        kind: 'object',
        value: {
          fn: () => 1,
        },
      } as never),
    ).rejects.toBeInstanceOf(SerializationError);
  });

  it('fails workflow when step result is non-serializable', async () => {
    const jobId = await engine.start('non-serializable-step-result-workflow', {});

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
});
