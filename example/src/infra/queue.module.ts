import { Global, Module } from '@nestjs/common';
import { queueProviders } from './queue.providers';

@Global()
@Module({
  providers: [...queueProviders],
  exports: [...queueProviders],
})
export class QueueModule {}
