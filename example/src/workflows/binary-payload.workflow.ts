import { DozerWorkflow, Step, Workflow } from 'dozer';
import { FailureMemoryService } from '../support/failure-memory.service';

@Workflow({ name: 'binary-payload' })
export class BinaryPayloadWorkflow extends DozerWorkflow<{ id: string; bytes: Uint8Array; arrayBuffer: ArrayBuffer; buffer: Buffer; blob?: Blob }> {
  constructor(private readonly failureMemory: FailureMemoryService) {
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
    sum: number;
    isUint8Array: boolean;
    isArrayBuffer: boolean;
    isBuffer: boolean;
    blobSize: number;
  }> {
    this.failureMemory.markAndShouldFail(
      `binary-payload:inspect:${input.id}`,
      0,
    );

    return Promise.resolve({
      sum: input.bytes.reduce((acc, value) => acc + value, 0),
      isUint8Array: input.bytes instanceof Uint8Array,
      isArrayBuffer: input.arrayBuffer instanceof ArrayBuffer,
      isBuffer: Buffer.isBuffer(input.buffer),
      blobSize: input.blob?.size ?? 0,
    });
  }

  @Step({ name: 'fail-once' })
  failOnce(id: string): Promise<void> {
    const shouldFail = this.failureMemory.markAndShouldFail(
      `binary-payload:fail:${id}`,
      1,
    );
    if (shouldFail) {
      return Promise.reject(new Error('binary-payload-fail-once'));
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
    sum: number;
    isUint8Array: boolean;
    isArrayBuffer: boolean;
    isBuffer: boolean;
    blobSize: number;
  }> {
    const inspected = await this.inspect(input);
    await this.failOnce(input.id);
    return inspected;
  }
}
