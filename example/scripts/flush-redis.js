/* eslint-disable no-console */
const Redis = require('ioredis');

const host = process.env.REDIS_HOST || '127.0.0.1';
const port = Number(process.env.REDIS_PORT || 6379);
const mode = String(process.env.REDIS_MODE || '').toLowerCase();
const clusterEnabled = mode === 'cluster' || process.env.REDIS_CLUSTER === '1';
const clusterNodesRaw = process.env.REDIS_CLUSTER_NODES || '';
const username = process.env.REDIS_USERNAME;
const password = process.env.REDIS_PASSWORD;
const db =
  process.env.REDIS_DB === undefined ? undefined : Number(process.env.REDIS_DB);
const tlsEnabled = process.env.REDIS_TLS === '1';
const required = process.env.REDIS_REQUIRED === '1';

const parseClusterNodes = (value) => {
  if (!value) {
    return [];
  }

  return value
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0)
    .map((item) => {
      const [nodeHost, nodePortRaw] = item.split(':');
      const nodePort = Number(nodePortRaw || 6379);
      if (!nodeHost) {
        return null;
      }

      return {
        host: nodeHost,
        port: Number.isFinite(nodePort) ? nodePort : 6379,
      };
    })
    .filter(Boolean);
};

const clusterNodes = parseClusterNodes(clusterNodesRaw);
const shouldUseCluster = clusterEnabled || clusterNodes.length > 0;

async function flushRedis() {
  if (shouldUseCluster) {
    const nodes = clusterNodes.length > 0 ? clusterNodes : [{ host, port }];
    const cluster = new Redis.Cluster(nodes, {
      clusterRetryStrategy: () => null,
      slotsRefreshTimeout: 2000,
      redisOptions: {
        maxRetriesPerRequest: 1,
        enableOfflineQueue: false,
        connectTimeout: 2000,
        ...(username ? { username } : {}),
        ...(password ? { password } : {}),
        ...(Number.isFinite(db) ? { db } : {}),
        ...(tlsEnabled ? { tls: {} } : {}),
      },
    });
    cluster.on('error', () => {
      // Connection errors are handled in try/catch below.
    });

    try {
      await cluster.ping();
      const masters = cluster.nodes('master');
      for (const node of masters) {
        await node.flushdb();
      }
      console.log(
        `Redis cluster flushed at ${nodes
          .map((node) => `${node.host}:${node.port}`)
          .join(',')}`,
      );
    } catch (error) {
      const message = `Unable to flush Redis cluster at ${nodes
        .map((node) => `${node.host}:${node.port}`)
        .join(',')}`;
      if (required) {
        console.error(`${message}:`, error);
        process.exitCode = 1;
        return;
      }

      console.warn(`${message}. Continuing without flush.`);
    } finally {
      try {
        await cluster.quit();
      } catch {
        cluster.disconnect();
      }
    }
    return;
  }

  const redis = new Redis({
    host,
    port,
    lazyConnect: true,
    connectTimeout: 2000,
    maxRetriesPerRequest: 1,
    enableOfflineQueue: false,
    ...(username ? { username } : {}),
    ...(password ? { password } : {}),
    ...(Number.isFinite(db) ? { db } : {}),
    ...(tlsEnabled ? { tls: {} } : {}),
  });
  redis.on('error', () => {
    // Connection errors are handled in try/catch below.
  });

  try {
    await redis.connect();
    await redis.flushdb();
    console.log(`Redis flushed at ${host}:${port}`);
  } catch (error) {
    const message = `Unable to flush Redis at ${host}:${port}`;
    if (required) {
      console.error(`${message}:`, error);
      process.exitCode = 1;
      return;
    }

    console.warn(`${message}. Continuing without flush.`);
  } finally {
    redis.disconnect();
  }
}

void flushRedis();
