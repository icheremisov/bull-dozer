import type { ConnectionOptions } from 'bullmq';
import Redis, {
  type Cluster,
  type ClusterOptions,
  type RedisOptions,
} from 'ioredis';

export interface ExampleRedisNode {
  host: string;
  port: number;
}

export interface StandaloneRedisConfig {
  mode: 'single';
  connection: RedisOptions;
}

export interface ClusterRedisConfig {
  mode: 'cluster';
  nodes: ExampleRedisNode[];
  options: ClusterOptions;
}

export type ExampleRedisConfig = StandaloneRedisConfig | ClusterRedisConfig;
export type ExampleRedisClient = Redis | Cluster;

const parsePort = (value: string | undefined, fallback: number): number => {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const parseClusterNodes = (value: string | undefined): ExampleRedisNode[] => {
  if (!value) {
    return [];
  }

  return value
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0)
    .map((item) => {
      const parts = item.split(':');
      const host = parts[0]?.trim();
      const port = parsePort(parts[1], 6379);
      if (!host) {
        return undefined;
      }

      return { host, port };
    })
    .filter((node): node is ExampleRedisNode => node !== undefined);
};

const redisHost = process.env.REDIS_HOST ?? '127.0.0.1';
const redisPort = parsePort(process.env.REDIS_PORT, 6379);
const redisDbRaw = process.env.REDIS_DB;
const redisDbParsed = redisDbRaw === undefined ? undefined : Number(redisDbRaw);
const redisDb =
  redisDbParsed !== undefined &&
  Number.isFinite(redisDbParsed) &&
  redisDbParsed >= 0
    ? Math.floor(redisDbParsed)
    : undefined;
const redisUsername = process.env.REDIS_USERNAME;
const redisPassword = process.env.REDIS_PASSWORD;
const redisTlsEnabled = process.env.REDIS_TLS === '1';
const redisMode = (process.env.REDIS_MODE ?? '').trim().toLowerCase();
const clusterNodes = parseClusterNodes(process.env.REDIS_CLUSTER_NODES);
const forceCluster =
  redisMode === 'cluster' || process.env.REDIS_CLUSTER === '1';

const sharedRedisOptions: RedisOptions = {
  maxRetriesPerRequest: null,
  enableOfflineQueue: false,
  enableReadyCheck: false,
  ...(redisUsername ? { username: redisUsername } : {}),
  ...(redisPassword ? { password: redisPassword } : {}),
  ...(redisDb !== undefined ? { db: redisDb } : {}),
  ...(redisTlsEnabled ? { tls: {} } : {}),
};

const resolveRedisConfig = (): ExampleRedisConfig => {
  const shouldUseCluster = forceCluster || clusterNodes.length > 0;
  if (shouldUseCluster) {
    const nodes =
      clusterNodes.length > 0
        ? clusterNodes
        : [{ host: redisHost, port: redisPort }];
    return {
      mode: 'cluster',
      nodes,
      options: {
        redisOptions: sharedRedisOptions,
      },
    };
  }

  return {
    mode: 'single',
    connection: {
      ...sharedRedisOptions,
      host: redisHost,
      port: redisPort,
    },
  };
};

export const exampleRedisConfig: ExampleRedisConfig = resolveRedisConfig();

export const createBullMqConnection = (
  config: ExampleRedisConfig,
): ConnectionOptions => {
  if (config.mode === 'cluster') {
    return new Redis.Cluster(
      config.nodes,
      config.options,
    ) as unknown as ConnectionOptions;
  }

  return config.connection as unknown as ConnectionOptions;
};

export const createRedisClient = (
  config: ExampleRedisConfig,
): ExampleRedisClient => {
  if (config.mode === 'cluster') {
    return new Redis.Cluster(config.nodes, config.options);
  }

  return new Redis(config.connection);
};

export const isRedisClientConnection = (
  connection: unknown,
): connection is ExampleRedisClient => {
  const candidate = connection as Partial<ExampleRedisClient> | undefined;
  return (
    typeof candidate?.quit === 'function' &&
    typeof candidate?.disconnect === 'function'
  );
};

export const disconnectRedisClient = async (
  client: ExampleRedisClient,
): Promise<void> => {
  try {
    await client.quit();
  } catch {
    client.disconnect();
  }
};

export const redisTargetLabel = (config: ExampleRedisConfig): string => {
  if (config.mode === 'cluster') {
    return `cluster(${config.nodes
      .map((node) => `${node.host}:${node.port}`)
      .join(',')})`;
  }

  return `${config.connection.host}:${config.connection.port}`;
};
