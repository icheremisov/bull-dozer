import net from 'node:net';

export interface RedisNodeAddress {
  host: string;
  port: number;
}

export interface RedisTestConfig {
  host: string;
  port: number;
  mode: 'single' | 'cluster';
  nodes: RedisNodeAddress[];
  target: string;
  required: boolean;
}

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

const parsePort = (value: string | undefined, fallback: number): number => {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const parseClusterNodes = (value: string | undefined): RedisNodeAddress[] => {
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
    .filter((node): node is RedisNodeAddress => node !== undefined);
};

const standaloneHost = process.env.REDIS_HOST ?? '127.0.0.1';
const standalonePort = parsePort(process.env.REDIS_PORT, 6379);
const redisMode = (process.env.REDIS_MODE ?? '').trim().toLowerCase();
const forceCluster =
  redisMode === 'cluster' || process.env.REDIS_CLUSTER === '1';
const clusterNodes = parseClusterNodes(process.env.REDIS_CLUSTER_NODES);
const mode: 'single' | 'cluster' =
  forceCluster || clusterNodes.length > 0 ? 'cluster' : 'single';
const nodes =
  mode === 'cluster'
    ? clusterNodes.length > 0
      ? clusterNodes
      : [{ host: standaloneHost, port: standalonePort }]
    : [{ host: standaloneHost, port: standalonePort }];
const firstNode = nodes[0] ?? { host: standaloneHost, port: standalonePort };

export const redisTestConfig: RedisTestConfig = {
  host: firstNode.host,
  port: firstNode.port,
  mode,
  nodes,
  target:
    mode === 'cluster'
      ? `cluster(${nodes.map((node) => `${node.host}:${node.port}`).join(',')})`
      : `${firstNode.host}:${firstNode.port}`,
  required: process.env.REDIS_REQUIRED === '1',
};

const canConnect = async (node: RedisNodeAddress): Promise<boolean> => {
  return new Promise<boolean>((resolve) => {
    const socket = new net.Socket();

    const finish = (ok: boolean): void => {
      socket.removeAllListeners();
      socket.destroy();
      resolve(ok);
    };

    socket.setTimeout(700);
    socket.once('connect', () => finish(true));
    socket.once('timeout', () => finish(false));
    socket.once('error', () => finish(false));
    socket.connect(node.port, node.host);
  });
};

export const isRedisReachable = async (
  retries = 6,
  delayMs = 250,
): Promise<boolean> => {
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      const checks = await Promise.all(
        redisTestConfig.nodes.map((node) => canConnect(node)),
      );
      const reachable = checks.every(Boolean);

      if (reachable) {
        return true;
      }
    } catch {
      // Redis may be temporarily unavailable while container boots.
    }

    if (attempt < retries) {
      await sleep(delayMs);
    }
  }

  return false;
};
