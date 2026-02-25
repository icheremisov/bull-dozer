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
const workflowQueueName =
  (process.env.EXAMPLE_WORKFLOW_QUEUE_NAME || 'wf-example').trim();
const resultQueueName =
  (process.env.EXAMPLE_RESULT_QUEUE_NAME || `${workflowQueueName}-results`).trim();

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

const makeRedisOptions = () => ({
  maxRetriesPerRequest: 1,
  enableOfflineQueue: false,
  enableReadyCheck: false,
  connectTimeout: 2000,
  ...(username ? { username } : {}),
  ...(password ? { password } : {}),
  ...(Number.isFinite(db) ? { db } : {}),
  ...(tlsEnabled ? { tls: {} } : {}),
});

const scanAndDelete = async (client, pattern) => {
  let cursor = '0';
  let deleted = 0;

  do {
    const [nextCursor, keys] = await client.scan(
      cursor,
      'MATCH',
      pattern,
      'COUNT',
      500,
    );
    cursor = nextCursor;
    if (Array.isArray(keys) && keys.length > 0) {
      deleted += await client.del(...keys);
    }
  } while (cursor !== '0');

  return deleted;
};

const cleanNodeQueues = async (client) => {
  const patterns = [
    `bull:${workflowQueueName}:*`,
    `bull:${resultQueueName}:*`,
  ];

  let deleted = 0;
  for (const pattern of patterns) {
    deleted += await scanAndDelete(client, pattern);
  }
  return deleted;
};

async function cleanQueues() {
  if (shouldUseCluster) {
    const nodes = clusterNodes.length > 0 ? clusterNodes : [{ host, port }];
    const cluster = new Redis.Cluster(nodes, {
      clusterRetryStrategy: () => null,
      slotsRefreshTimeout: 2000,
      redisOptions: makeRedisOptions(),
    });
    cluster.on('error', () => {
      // Connection errors are handled in try/catch below.
    });

    try {
      await cluster.ping();
      let deleted = 0;
      for (const node of cluster.nodes('master')) {
        deleted += await cleanNodeQueues(node);
      }
      console.log(
        `Cleaned ${deleted} Redis key(s) for queues "${workflowQueueName}" and "${resultQueueName}" in cluster ${nodes
          .map((node) => `${node.host}:${node.port}`)
          .join(',')}`,
      );
    } catch (error) {
      const message = `Unable to clean Redis queue keys in cluster ${nodes
        .map((node) => `${node.host}:${node.port}`)
        .join(',')}`;
      if (required) {
        console.error(`${message}:`, error);
        process.exitCode = 1;
        return;
      }

      console.warn(`${message}. Continuing without cleanup.`);
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
    ...makeRedisOptions(),
  });
  redis.on('error', () => {
    // Connection errors are handled in try/catch below.
  });

  try {
    await redis.connect();
    const deleted = await cleanNodeQueues(redis);
    console.log(
      `Cleaned ${deleted} Redis key(s) for queues "${workflowQueueName}" and "${resultQueueName}" at ${host}:${port}`,
    );
  } catch (error) {
    const message = `Unable to clean Redis queue keys at ${host}:${port}`;
    if (required) {
      console.error(`${message}:`, error);
      process.exitCode = 1;
      return;
    }

    console.warn(`${message}. Continuing without cleanup.`);
  } finally {
    redis.disconnect();
  }
}

void cleanQueues();
