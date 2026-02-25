import { Module } from '@nestjs/common';
import { Queue } from 'bullmq';
import { DozerModule } from 'dozer';
import { QueueModule } from './infra/queue.module';
import { EXAMPLE_RESULT_QUEUE, EXAMPLE_WORKFLOW_QUEUE } from './infra/tokens';
import { ClientBatchService } from './client/client-batch.service';
import { ClientController } from './client/client.controller';
import { ClientResultWorkerService } from './client/client-result-worker.service';

@Module({
  imports: [
    QueueModule,
    DozerModule.forClientAsync({
      imports: [QueueModule],
      inject: [EXAMPLE_WORKFLOW_QUEUE, EXAMPLE_RESULT_QUEUE],
      useFactory: (queue: Queue, resultQueue: Queue) => ({
        queue,
        resultQueue,
      }),
    }),
  ],
  controllers: [ClientController],
  providers: [ClientBatchService, ClientResultWorkerService],
  exports: [ClientBatchService],
})
export class ClientAppModule {}
