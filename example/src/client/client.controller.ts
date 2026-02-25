import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { ClientBatchService, type BatchSnapshot } from './client-batch.service';

interface StartBatchRequest {
  workflowName: string;
  inputs: unknown[];
}

@Controller('client')
export class ClientController {
  constructor(private readonly batches: ClientBatchService) {}

  @Post('batches/start')
  async startBatch(
    @Body() body: StartBatchRequest,
  ): Promise<{ batchId: string; jobIds: string[] }> {
    const workflowName =
      typeof body?.workflowName === 'string' ? body.workflowName : '';
    const inputs = Array.isArray(body?.inputs) ? body.inputs : [];
    if (!workflowName) {
      throw new Error('"workflowName" is required.');
    }

    return this.batches.submitBatch(workflowName, inputs);
  }

  @Get('batches/:batchId')
  getBatch(@Param('batchId') batchId: string):
    | { found: false }
    | {
        found: true;
        batch: BatchSnapshot;
      } {
    const batch = this.batches.getBatch(batchId);
    if (!batch) {
      return { found: false };
    }

    return {
      found: true,
      batch,
    };
  }
}
