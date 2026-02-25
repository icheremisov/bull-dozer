import { Injectable } from '@nestjs/common';
import {
  DozerClient,
  type WaitForWorkflowResultOptions,
  type WorkflowResultMessage,
} from 'dozer';

interface BatchItemResult {
  workflowJobId: string;
  workflowName: string;
  result: unknown;
  receivedAt: string;
}

interface BatchRecord {
  id: string;
  workflowName: string;
  jobIds: string[];
  resultsByJobId: Map<string, BatchItemResult>;
  createdAt: string;
}

export interface BatchSnapshot {
  id: string;
  workflowName: string;
  jobIds: string[];
  total: number;
  completed: number;
  pending: number;
  createdAt: string;
  results: BatchItemResult[];
}

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

@Injectable()
export class ClientBatchService {
  private readonly batches = new Map<string, BatchRecord>();
  private readonly jobToBatch = new Map<string, string>();
  private readonly orphanResults = new Map<string, BatchItemResult>();
  private batchCounter = 0;

  constructor(private readonly client: DozerClient) {}

  async submitBatch(
    workflowName: string,
    inputs: unknown[],
  ): Promise<{ batchId: string; jobIds: string[] }> {
    const batchId = `batch-${++this.batchCounter}`;
    const jobIds = await Promise.all(
      inputs.map((input) => this.client.start(workflowName, input)),
    );

    const record: BatchRecord = {
      id: batchId,
      workflowName,
      jobIds,
      resultsByJobId: new Map<string, BatchItemResult>(),
      createdAt: new Date().toISOString(),
    };
    this.batches.set(batchId, record);
    for (const jobId of jobIds) {
      this.jobToBatch.set(jobId, batchId);
      const orphan = this.orphanResults.get(jobId);
      if (orphan) {
        record.resultsByJobId.set(jobId, orphan);
        this.orphanResults.delete(jobId);
      }
    }

    return { batchId, jobIds };
  }

  recordResult(message: WorkflowResultMessage<unknown>): void {
    const batchId = this.jobToBatch.get(message.workflowJobId);
    if (!batchId) {
      this.orphanResults.set(message.workflowJobId, {
        workflowJobId: message.workflowJobId,
        workflowName: message.workflowName,
        result: message.result,
        receivedAt: new Date().toISOString(),
      });
      return;
    }

    const batch = this.batches.get(batchId);
    if (!batch) {
      return;
    }

    batch.resultsByJobId.set(message.workflowJobId, {
      workflowJobId: message.workflowJobId,
      workflowName: message.workflowName,
      result: message.result,
      receivedAt: new Date().toISOString(),
    });
  }

  getBatch(batchId: string): BatchSnapshot | null {
    const batch = this.batches.get(batchId);
    if (!batch) {
      return null;
    }

    const results = batch.jobIds
      .map((jobId) => batch.resultsByJobId.get(jobId))
      .filter((item): item is BatchItemResult => item !== undefined);

    return {
      id: batch.id,
      workflowName: batch.workflowName,
      jobIds: [...batch.jobIds],
      total: batch.jobIds.length,
      completed: results.length,
      pending: batch.jobIds.length - results.length,
      createdAt: batch.createdAt,
      results,
    };
  }

  getBatchResultRaw(batchId: string, workflowJobId: string): unknown {
    return this.batches.get(batchId)?.resultsByJobId.get(workflowJobId)?.result;
  }

  async waitForBatch(
    batchId: string,
    options?: WaitForWorkflowResultOptions,
  ): Promise<BatchSnapshot> {
    const timeoutMs = options?.timeoutMs ?? 15000;
    const pollMs = options?.pollMs ?? 100;
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      const snapshot = this.getBatch(batchId);
      if (!snapshot) {
        throw new Error(`Batch "${batchId}" not found.`);
      }
      if (snapshot.pending === 0) {
        return snapshot;
      }

      await sleep(pollMs);
    }

    throw new Error(`Timed out waiting for batch "${batchId}" completion.`);
  }

  reset(): void {
    this.batches.clear();
    this.jobToBatch.clear();
    this.orphanResults.clear();
    this.batchCounter = 0;
  }
}
