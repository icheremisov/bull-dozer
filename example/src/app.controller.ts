import { Body, Controller, Get, Inject, Param, Post } from '@nestjs/common';
import { Queue } from 'bullmq';
import { DozerEngine, DOZER_JOB_STATE_KEY, WorkflowJobData } from 'dozer';
import { BranchSelectorService } from './support/branch-selector.service';
import { EXAMPLE_WORKFLOW_QUEUE } from './infra/tokens';

@Controller('workflows')
export class AppController {
  constructor(
    private readonly engine: DozerEngine,
    private readonly branchSelector: BranchSelectorService,
    @Inject(EXAMPLE_WORKFLOW_QUEUE)
    private readonly queue: Queue<WorkflowJobData<unknown>>,
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

  @Post('branch/:key/:branch')
  setBranch(
    @Param('key') key: string,
    @Param('branch') branch: 'left' | 'right',
  ): { ok: true } {
    this.branchSelector.setBranch(key, branch);
    return { ok: true };
  }
}
