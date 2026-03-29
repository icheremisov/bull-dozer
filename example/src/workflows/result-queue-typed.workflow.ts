import { DozerWorkflow, Workflow } from 'dozer';

@Workflow({
  name: 'result-queue-typed',
  resultQueue: {
    jobName: 'workflow-result-typed',
    job: {
      removeOnComplete: false,
    },
  },
})
export class ResultQueueTypedWorkflow extends DozerWorkflow<{
  id: string;
  seed: number;
}> {
  run(input: { id: string; seed: number }): Promise<{
    id: string;
    seed: number;
    at: Date;
    bytes: Uint8Array;
    arrayBuffer: ArrayBuffer;
    buffer: Buffer;
    view: DataView;
    blob?: Blob;
    nested: {
      optional?: string;
      list: Array<Date | Uint8Array>;
    };
  }> {
    const bytes = new Uint8Array([input.seed, input.seed + 1, input.seed + 2]);
    const arrayBuffer = bytes.buffer.slice(0);
    const buffer = Buffer.from([input.seed + 3, input.seed + 4]);
    const view = new DataView(arrayBuffer);
    const blob =
      typeof Blob === 'undefined'
        ? undefined
        : new Blob([arrayBuffer], { type: 'application/octet-stream' });

    return Promise.resolve({
      id: input.id,
      seed: input.seed,
      at: new Date('2026-02-25T12:34:56.000Z'),
      bytes,
      arrayBuffer,
      buffer,
      view,
      blob,
      nested: {
        optional: undefined,
        list: [new Date('2026-02-25T00:00:00.000Z'), new Uint8Array([9, 8, 7])],
      },
    });
  }
}
