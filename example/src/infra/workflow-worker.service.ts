import {
  Inject,
  Injectable,
  OnApplicationBootstrap,
  OnModuleDestroy,
} from '@nestjs/common';
import { Worker, type ConnectionOptions } from 'bullmq';
import { DozerEngine } from 'dozer';
import {
  EXAMPLE_REDIS_CONNECTION,
  EXAMPLE_WORKFLOW_QUEUE_NAME,
} from './tokens';

@Injectable()
export class WorkflowWorkerService
  implements OnApplicationBootstrap, OnModuleDestroy
{
  private worker?: Worker;

  constructor(
    private readonly engine: DozerEngine,
    @Inject(EXAMPLE_REDIS_CONNECTION)
    private readonly connection: ConnectionOptions,
  ) {}

  onApplicationBootstrap(): void {
    this.worker = new Worker(
      EXAMPLE_WORKFLOW_QUEUE_NAME,
      async (job) => {
        await this.engine.run(String(job.id));
      },
      {
        connection: this.connection,
        concurrency: 10,
      },
    );
  }

  async onModuleDestroy(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
      this.worker = undefined;
    }
  }
}
