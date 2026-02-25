import {
  Inject,
  Injectable,
  OnApplicationShutdown,
  type Provider,
} from '@nestjs/common';
import { Queue, type ConnectionOptions } from 'bullmq';
import {
  createBullMqConnection,
  disconnectRedisClient,
  exampleRedisConfig,
  isRedisClientConnection,
  type ExampleRedisConfig,
} from './redis-config';
import {
  EXAMPLE_REDIS_CONFIG,
  EXAMPLE_REDIS_CONNECTION,
  EXAMPLE_RESULT_QUEUE,
  EXAMPLE_RESULT_QUEUE_NAME,
  EXAMPLE_WORKFLOW_QUEUE,
  EXAMPLE_WORKFLOW_QUEUE_NAME,
} from './tokens';

@Injectable()
class QueueLifecycleService implements OnApplicationShutdown {
  constructor(
    @Inject(EXAMPLE_WORKFLOW_QUEUE)
    private readonly queue: Queue,
    @Inject(EXAMPLE_RESULT_QUEUE)
    private readonly resultQueue: Queue,
    @Inject(EXAMPLE_REDIS_CONNECTION)
    private readonly connection: ConnectionOptions,
  ) {}

  async onApplicationShutdown(): Promise<void> {
    await this.queue.close().catch(() => {
      // Queue may already be closed by test/perf runner code.
    });
    await this.resultQueue.close().catch(() => {
      // Queue may already be closed by test/perf runner code.
    });

    if (isRedisClientConnection(this.connection)) {
      await disconnectRedisClient(this.connection);
    }
  }
}

export const queueProviders: Provider[] = [
  {
    provide: EXAMPLE_REDIS_CONFIG,
    useValue: exampleRedisConfig,
  },
  {
    provide: EXAMPLE_REDIS_CONNECTION,
    useFactory: (config: ExampleRedisConfig): ConnectionOptions => {
      return createBullMqConnection(config);
    },
    inject: [EXAMPLE_REDIS_CONFIG],
  },
  {
    provide: EXAMPLE_WORKFLOW_QUEUE,
    useFactory: (connection: ConnectionOptions): Queue => {
      return new Queue(EXAMPLE_WORKFLOW_QUEUE_NAME, {
        connection,
      });
    },
    inject: [EXAMPLE_REDIS_CONNECTION],
  },
  {
    provide: EXAMPLE_RESULT_QUEUE,
    useFactory: (connection: ConnectionOptions): Queue => {
      return new Queue(EXAMPLE_RESULT_QUEUE_NAME, {
        connection,
      });
    },
    inject: [EXAMPLE_REDIS_CONNECTION],
  },
  QueueLifecycleService,
];
