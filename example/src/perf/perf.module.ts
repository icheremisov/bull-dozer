import { Module } from '@nestjs/common';
import { Queue } from 'bullmq';
import { DozerModule } from 'dozer';
import {
  createBullMqConnection,
  exampleRedisConfig,
} from '../infra/redis-config';
import {
  EXAMPLE_REDIS_CONFIG,
  EXAMPLE_REDIS_CONNECTION,
  EXAMPLE_WORKFLOW_QUEUE,
} from '../infra/tokens';
import { WorkflowJoinService } from '../support/workflow-join.service';
import { PerfFailureService } from './services/perf-failure.service';
import { PerfChildWorkflow } from './workflows/perf-child.workflow';
import { PerfMainWorkflow } from './workflows/perf-main.workflow';

const perfQueueName = process.env.PERF_WORKFLOW_QUEUE_NAME ?? 'wf-perf';
const perfConnection = createBullMqConnection(exampleRedisConfig);
const perfQueue = new Queue(perfQueueName, {
  connection: perfConnection,
});

@Module({
  imports: [
    DozerModule.forRoot({
      queue: perfQueue,
    }),
    DozerModule.forFeature(
      [PerfMainWorkflow, PerfChildWorkflow],
      [
        {
          provide: EXAMPLE_REDIS_CONFIG,
          useValue: exampleRedisConfig,
        },
        {
          provide: EXAMPLE_REDIS_CONNECTION,
          useValue: perfConnection,
        },
        {
          provide: EXAMPLE_WORKFLOW_QUEUE,
          useValue: perfQueue,
        },
        WorkflowJoinService,
        PerfFailureService,
      ],
    ),
  ],
})
export class PerfModule {}
