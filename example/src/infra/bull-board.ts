import { INestApplication } from '@nestjs/common';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { createBullBoard } from '@bull-board/api';
import { ExpressAdapter } from '@bull-board/express';
import { Queue, type ConnectionOptions } from 'bullmq';
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
const DISCOVERY_CONNECT_TIMEOUT_MS = 3000;

const waitForRedisClientReady = async (
  client: ExampleRedisClient,
): Promise<void> => {
  const candidate = client as unknown as {
    status?: string;
    once?: (event: string, listener: (...args: unknown[]) => void) => unknown;
    off?: (event: string, listener: (...args: unknown[]) => void) => unknown;
  };
  if (candidate.status === 'ready') {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    const onReady = (): void => {
      cleanup();
      resolve();
    };
    const onError = (error: unknown): void => {
      cleanup();
      reject(error instanceof Error ? error : new Error(String(error)));
    };
    const onClose = (): void => {
      cleanup();
      reject(new Error('Redis discovery client closed before connect.'));
    };
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error('Timed out waiting for Redis discovery client.'));
    }, DISCOVERY_CONNECT_TIMEOUT_MS);

    const cleanup = (): void => {
      clearTimeout(timeout);
      candidate.off?.('ready', onReady);
      candidate.off?.('error', onError);
      candidate.off?.('close', onClose);
      candidate.off?.('end', onClose);
    };

    candidate.once?.('ready', onReady);
    candidate.once?.('error', onError);
    candidate.once?.('close', onClose);
    candidate.once?.('end', onClose);
  });
};

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
  queues: Queue | Queue[],
  connection?: ConnectionOptions,
  redisConfig?: ExampleRedisConfig,
): void => {
  const serverAdapter = new ExpressAdapter();
  serverAdapter.setBasePath(BULL_BOARD_BASE_PATH);

  const knownQueues: Queue[] = Array.isArray(queues) ? queues : [queues];
  const queueRegistry = new Map<string, Queue>(
    knownQueues.map((queue) => [queue.name, queue]),
  );
  const getQueue = (name: string): Queue => {
    const existing = queueRegistry.get(name);
    if (existing) {
      return existing;
    }

    if (!connection) {
      return knownQueues[0];
    }

    const created = new Queue(name, {
      connection,
    });
    queueRegistry.set(name, created);
    return created;
  };
  const toAdapters = (queueNames: string[]): BullMQAdapter[] => {
    return queueNames.map((name) => new BullMQAdapter(getQueue(name)));
  };

  const board = createBullBoard({
    queues: toAdapters(Array.from(queueRegistry.keys())),
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
      const clientWithConnect = temporaryClient as unknown as {
        connect?: () => Promise<unknown>;
      };
      if (typeof clientWithConnect.connect === 'function') {
        try {
          await clientWithConnect.connect();
        } catch {
          // Auto-connect may already be in progress/connected.
        }
      }
      await waitForRedisClientReady(temporaryClient);

      return await callback(temporaryClient);
    } finally {
      await disconnectRedisClient(temporaryClient);
    }
  };

  const syncQueues = async (): Promise<void> => {
    const discoveredQueues =
      (await withDiscoveryClient(discoverQueueNames)) ?? [];
    const queueNames = Array.from(
      new Set<string>([...queueRegistry.keys(), ...discoveredQueues]),
    );
    board.setQueues(toAdapters(queueNames));
  };

  void syncQueues().catch((error: unknown) => {
    // Keep dashboard alive even if discovery fails.
    if (process.env.BULL_BOARD_DEBUG === '1') {
      console.warn('[bull-board] queue discovery failed', error);
    }
  });
  const refreshTimer = setInterval(() => {
    void syncQueues().catch((error: unknown) => {
      // Do not crash app on temporary Redis scan issues.
      if (process.env.BULL_BOARD_DEBUG === '1') {
        console.warn('[bull-board] queue discovery refresh failed', error);
      }
    });
  }, BULL_BOARD_REFRESH_MS);
  refreshTimer.unref();
};
