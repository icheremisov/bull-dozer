import { NestFactory } from '@nestjs/core';
import { Queue, type ConnectionOptions } from 'bullmq';
import { WorkflowJobData, WorkflowResultQueueJobData } from 'dozer';
import { AppModule } from './app.module';
import { setupBullBoard } from './infra/bull-board';
import {
  EXAMPLE_REDIS_CONFIG,
  EXAMPLE_REDIS_CONNECTION,
  EXAMPLE_RESULT_QUEUE,
  EXAMPLE_WORKFLOW_QUEUE,
} from './infra/tokens';
import { type ExampleRedisConfig } from './infra/redis-config';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  const queue = app.get<Queue<WorkflowJobData<unknown>>>(
    EXAMPLE_WORKFLOW_QUEUE,
  );
  const resultQueue =
    app.get<Queue<WorkflowResultQueueJobData<unknown>>>(EXAMPLE_RESULT_QUEUE);
  const redisConfig = app.get<ExampleRedisConfig>(EXAMPLE_REDIS_CONFIG);
  const connection = app.get<ConnectionOptions>(EXAMPLE_REDIS_CONNECTION);
  setupBullBoard(app, [queue, resultQueue], connection, redisConfig);
  await app.listen(process.env.PORT ?? 3100);
}

void bootstrap();
