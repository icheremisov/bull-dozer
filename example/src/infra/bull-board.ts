import { INestApplication } from '@nestjs/common';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { createBullBoard } from '@bull-board/api';
import { ExpressAdapter } from '@bull-board/express';
import { Queue, type ConnectionOptions } from 'bullmq';
import { WorkflowJobData } from 'dozer';
import Redis, { type RedisOptions } from 'ioredis';
import {
  createRedisClient,
  disconnectRedisClient,
  isRedisClientConnection,
  type ExampleRedisClient,
  type ExampleRedisConfig,
} from './redis-config';

export const BULL_BOARD_BASE_PATH = '/admin/queues';
const BULL_BOARD_REFRESH_MS = 5000;

const collectQueueNamesFromScan = async (
  redis: Redis,
  queueNames: Set<string>,
): Promise<void> => {
  let cursor = '0';

  do {
    const [nextCursor, keys] = await redis.scan(
      cursor,
      'MATCH',
      'bull:*:*',
      'COUNT',
      '500',
    );
    cursor = nextCursor;

    for (const key of keys) {
      const parts = key.split(':');
      if (parts.length >= 3 && parts[0] === 'bull' && parts[1]) {
        queueNames.add(parts[1]);
      }
    }
  } while (cursor !== '0');
};

const discoverQueueNames = async (
  redis: ExampleRedisClient,
): Promise<string[]> => {
  const queueNames = new Set<string>();
  const clusterCandidate = redis as {
    nodes?: (role: 'master' | 'slave') => Redis[];
  };
  const isClusterClient = typeof clusterCandidate.nodes === 'function';

  if (isClusterClient) {
    await (clusterCandidate as { ping?: () => Promise<string> })
      .ping?.()
      .catch(() => undefined);
    const masters = clusterCandidate.nodes?.('master') ?? [];
    for (const node of masters) {
      await collectQueueNamesFromScan(node, queueNames);
    }
    return Array.from(queueNames);
  }

  await collectQueueNamesFromScan(redis as Redis, queueNames);
  return Array.from(queueNames);
};

export const setupBullBoard = (
  app: INestApplication,
  queue: Queue<WorkflowJobData<unknown>>,
  connection?: ConnectionOptions,
  redisConfig?: ExampleRedisConfig,
): void => {
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath(BULL_BOARD_BASE_PATH);

  const queueRegistry = new Map<string, Queue<unknown>>([
    [queue.name, queue as unknown as Queue<unknown>],
  ]);
  const getQueue = (name: string): Queue<unknown> => {
    const existing = queueRegistry.get(name);
    if (existing) {
      return existing;
    }

    if (!connection) {
      return queue as unknown as Queue<unknown>;
    }

    const created = new Queue(name, {
      connection,
    }) as unknown as Queue<unknown>;
    queueRegistry.set(name, created);
    return created;
  };
  const toAdapters = (queueNames: string[]): BullMQAdapter[] => {
    return queueNames.map((name) => new BullMQAdapter(getQueue(name)));
  };

  const board = createBullBoard({
    queues: toAdapters([queue.name]),
    serverAdapter,
  });

  app.use(BULL_BOARD_BASE_PATH, serverAdapter.getRouter());
  app.use(`${BULL_BOARD_BASE_PATH}/`, serverAdapter.getRouter());

  if (!connection && !redisConfig) {
    return;
  }

  const withDiscoveryClient = async <T>(
    callback: (client: ExampleRedisClient) => Promise<T>,
  ): Promise<T | undefined> => {
    if (connection && isRedisClientConnection(connection)) {
      return callback(connection);
    }

    const temporaryClient = redisConfig
      ? createRedisClient(redisConfig)
      : connection
        ? new Redis(connection as unknown as RedisOptions)
        : undefined;

    if (!temporaryClient) {
      return undefined;
    }

    try {
      return await callback(temporaryClient);
    } finally {
      await disconnectRedisClient(temporaryClient);
    }
  };

  const syncQueues = async (): Promise<void> => {
    const discoveredQueues =
      (await withDiscoveryClient(discoverQueueNames)) ?? [];
    const queueNames = Array.from(
      new Set<string>([queue.name, ...discoveredQueues]),
    );
    board.setQueues(toAdapters(queueNames));
  };

  void syncQueues().catch(() => {
    // Keep dashboard alive even if discovery fails.
  });
  const refreshTimer = setInterval(() => {
    void syncQueues().catch(() => {
      // Do not crash app on temporary Redis scan issues.
    });
  }, BULL_BOARD_REFRESH_MS);
  refreshTimer.unref();
};
