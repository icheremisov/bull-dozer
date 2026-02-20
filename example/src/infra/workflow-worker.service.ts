import {
  Inject,
  Injectable,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Worker, type ConnectionOptions } from 'bullmq';
import { DozerEngine, WORKFLOW_QUEUE_NAME } from 'dozer';
import { EXAMPLE_REDIS_CONNECTION } from './tokens';

@Injectable()
export class WorkflowWorkerService implements OnModuleInit, OnModuleDestroy {
  private worker?: Worker;

  constructor(
    private readonly engine: DozerEngine,
    @Inject(EXAMPLE_REDIS_CONNECTION)
    private readonly connection: ConnectionOptions,
  ) {}

  onModuleInit(): void {
    this.worker = new Worker(
      WORKFLOW_QUEUE_NAME,
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
