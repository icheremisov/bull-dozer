import {
  Injectable,
  OnApplicationBootstrap,
  OnModuleDestroy,
} from '@nestjs/common';
import { DozerClient } from 'dozer';
import { ClientBatchService } from './client-batch.service';

@Injectable()
export class ClientResultWorkerService
  implements OnApplicationBootstrap, OnModuleDestroy
{
  private worker?: { close(): Promise<void> };

  constructor(
    private readonly client: DozerClient,
    private readonly batches: ClientBatchService,
  ) {}

  onApplicationBootstrap(): void {
    this.worker = this.client.createResultWorker((message) => {
      this.batches.recordResult(message);
      return undefined;
    });
  }

  async onModuleDestroy(): Promise<void> {
    if (this.worker) {
      await this.worker.close();
      this.worker = undefined;
    }
  }
}
