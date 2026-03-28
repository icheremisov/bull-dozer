import { Body, Controller, Get, Inject, Param, Post } from '@nestjs/common';
import { Queue } from 'bullmq';
import {
  DozerClient,
  DozerEngine,
  DOZER_JOB_STATE_KEY,
  WorkflowJobData,
  WorkflowResultQueueJobData,
} from 'dozer';
import { BranchSelectorService } from './support/branch-selector.service';
import { EXAMPLE_RESULT_QUEUE, EXAMPLE_WORKFLOW_QUEUE } from './infra/tokens';

const toResultQueueJobId = (workflowJobId: string): string => {
  if (/^\d+$/.test(workflowJobId)) {
    return `#${workflowJobId}`;
  }

  return workflowJobId;
};

@Controller('workflows')
export class AppController {
  constructor(
    private readonly engine: DozerEngine,
    private readonly dozerClient: DozerClient,
    private readonly branchSelector: BranchSelectorService,
    @Inject(EXAMPLE_WORKFLOW_QUEUE)
    private readonly queue: Queue<WorkflowJobData<unknown>>,
    @Inject(EXAMPLE_RESULT_QUEUE)
    private readonly resultQueue: Queue<WorkflowResultQueueJobData<unknown>>,
  ) {}

  @Post(':name/start')
  async start(
    @Param('name') name: string,
    @Body() input: Record<string, unknown>,
  ): Promise<{ jobId: string }> {
    const jobId = await this.engine.start(name, input);
    return { jobId };
  }

  @Post(':jobId/replay')
  async replay(@Param('jobId') jobId: string): Promise<{ result: unknown }> {
    const result = await this.engine.run(jobId);
    return { result };
  }

  @Get('results/:jobId')
  async resultStatus(
    @Param('jobId') jobId: string,
  ): Promise<Record<string, unknown>> {
    const resultJobId = toResultQueueJobId(jobId);
    const job =
      (await this.resultQueue.getJob(resultJobId)) ??
      (resultJobId === jobId ? null : await this.resultQueue.getJob(jobId));
    if (!job) {
      return {
        found: false,
      };
    }

    return {
      found: true,
      id: String(job.id),
      name: job.name,
      state: await job.getState(),
      data: job.data,
    };
  }

  @Get(':jobId')
  async status(
    @Param('jobId') jobId: string,
  ): Promise<Record<string, unknown>> {
    const job = await this.queue.getJob(jobId);
    if (!job) {
      return {
        found: false,
      };
    }

    return {
      found: true,
      id: String(job.id),
      name: job.name,
      state: await job.getState(),
      data: job.data,
      compactState: job.data[DOZER_JOB_STATE_KEY],
    };
  }

  @Post(':jobId/signal/:name')
  async sendSignal(
    @Param('jobId') jobId: string,
    @Param('name') name: string,
    @Body() body: Record<string, unknown>,
  ): Promise<{ delivered: boolean }> {
    const delivered = await this.dozerClient.sendSignal(jobId, name, body);
    return { delivered };
  }

  @Post('branch/:key/:branch')
  setBranch(
    @Param('key') key: string,
    @Param('branch') branch: 'left' | 'right',
  ): { ok: true } {
    this.branchSelector.setBranch(key, branch);
    return { ok: true };
  }
}
