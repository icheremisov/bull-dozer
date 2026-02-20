import 'reflect-metadata';
import { performance } from 'node:perf_hooks';
import { Test, TestingModule } from '@nestjs/testing';
import { Queue, Worker, type ConnectionOptions } from 'bullmq';
import {
  DOZER_JOB_STATE_KEY,
  DozerEngine,
  WORKFLOW_STATUS,
  WorkflowJobData,
} from 'dozer';
import { PerfModule } from '../src/perf/perf.module';
import { PerfMainInput } from '../src/perf/workflows/perf-main.workflow';
import { EXAMPLE_REDIS_CONNECTION, EXAMPLE_WORKFLOW_QUEUE } from '../src/infra/tokens';
import {
  disconnectRedisClient,
  isRedisClientConnection,
} from '../src/infra/redis-config';

interface PerfScenarioConfig {
  name: string;
  factor: string;
  factorValue: string;
  stepCount: number;
  payloadKb: number;
  jobCount: number;
  failureRate: number;
  nestedChildren: number;
  timerMs: number;
}

interface ScenarioResult {
  config: PerfScenarioConfig;
  completed: number;
  failed: number;
  avgLatencyMs: number;
  p50LatencyMs: number;
  p95LatencyMs: number;
  throughputJobsPerSec: number;
  wallMs: number;
}

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

const argValue = (name: string): string | undefined => {
  const inline = process.argv.find((arg) => arg.startsWith(`--${name}=`));
  if (inline) {
    const split = inline.split('=');
    return split[1];
  }

  const index = process.argv.indexOf(`--${name}`);
  if (index >= 0) {
    return process.argv[index + 1];
  }

  return undefined;
};

const intArg = (name: string, fallback: number): number => {
  const value = argValue(name);
  const parsed = value === undefined ? fallback : Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const floatArg = (name: string, fallback: number): number => {
  const value = argValue(name);
  const parsed = value === undefined ? fallback : Number.parseFloat(value);
  return Number.isFinite(parsed) ? parsed : fallback;
};

const boolArg = (name: string, fallback: boolean): boolean => {
  const value = argValue(name);
  if (value === undefined) {
    return fallback;
  }
  return value === '1' || value === 'true' || value === 'yes';
};

const listArg = (name: string, fallback: number[]): number[] => {
  const value = argValue(name);
  if (!value) {
    return fallback;
  }

  const parsed = value
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0)
    .map((item) => Number(item))
    .filter((item) => Number.isFinite(item));

  if (parsed.length === 0) {
    return fallback;
  }

  return Array.from(new Set(parsed));
};

const percentile = (values: number[], p: number): number => {
  if (values.length === 0) {
    return 0;
  }

  const sorted = [...values].sort((a, b) => a - b);
  const rank = Math.ceil((p / 100) * sorted.length) - 1;
  const index = Math.max(0, Math.min(sorted.length - 1, rank));
  return sorted[index];
};

const mapWithConcurrency = async <T>(
  total: number,
  concurrency: number,
  worker: (index: number) => Promise<T>,
): Promise<T[]> => {
  const results: T[] = new Array(total);
  let nextIndex = 0;

  const runLoop = async (): Promise<void> => {
    while (true) {
      const current = nextIndex;
      nextIndex += 1;
      if (current >= total) {
        return;
      }
      results[current] = await worker(current);
    }
  };

  const runners = Array.from(
    { length: Math.max(1, Math.min(concurrency, total)) },
    () => runLoop(),
  );

  await Promise.all(runners);
  return results;
};

const buildPayload = (payloadKb: number): string => {
  const bytes = Math.max(1, Math.floor(payloadKb * 1024));
  return 'x'.repeat(bytes);
};

const asStatus = (
  statusCode: number | undefined,
): 'completed' | 'failed' | 'pending' => {
  if (statusCode === WORKFLOW_STATUS.completed) {
    return 'completed';
  }
  if (statusCode === WORKFLOW_STATUS.failed) {
    return 'failed';
  }
  return 'pending';
};

const waitForTerminal = async (
  queue: Queue<WorkflowJobData<unknown>>,
  jobId: string,
  timeoutMs: number,
  pollMs: number,
): Promise<'completed' | 'failed'> => {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = await queue.getJob(jobId);
    const status = asStatus(job?.data?.[DOZER_JOB_STATE_KEY]?.s);
    if (status === 'completed' || status === 'failed') {
      return status;
    }
    await sleep(pollMs);
  }

  throw new Error(`Timed out waiting for workflow job ${jobId}`);
};

const buildSweepScenarios = (
  baseline: Omit<PerfScenarioConfig, 'name' | 'factor' | 'factorValue'>,
  lists: {
    steps: number[];
    payloadKb: number[];
    jobs: number[];
    failureRate: number[];
    nestedChildren: number[];
    timerMs: number[];
  },
): PerfScenarioConfig[] => {
  const scenarios: PerfScenarioConfig[] = [
    {
      ...baseline,
      name: 'baseline',
      factor: 'baseline',
      factorValue: '-',
    },
  ];

  for (const value of lists.steps) {
    const normalized = Math.max(1, Math.floor(value));
    if (normalized === baseline.stepCount) {
      continue;
    }
    scenarios.push({
      ...baseline,
      stepCount: normalized,
      name: `steps=${normalized}`,
      factor: 'steps',
      factorValue: String(normalized),
    });
  }

  for (const value of lists.payloadKb) {
    const normalized = Math.max(1, Math.floor(value));
    if (normalized === baseline.payloadKb) {
      continue;
    }
    scenarios.push({
      ...baseline,
      payloadKb: normalized,
      name: `payloadKb=${normalized}`,
      factor: 'payloadKb',
      factorValue: String(normalized),
    });
  }

  for (const value of lists.jobs) {
    const normalized = Math.max(1, Math.floor(value));
    if (normalized === baseline.jobCount) {
      continue;
    }
    scenarios.push({
      ...baseline,
      jobCount: normalized,
      name: `jobs=${normalized}`,
      factor: 'jobs',
      factorValue: String(normalized),
    });
  }

  for (const value of lists.failureRate) {
    const normalized = Math.min(1, Math.max(0, Number(value)));
    if (normalized === baseline.failureRate) {
      continue;
    }
    scenarios.push({
      ...baseline,
      failureRate: normalized,
      name: `failureRate=${normalized}`,
      factor: 'failureRate',
      factorValue: String(normalized),
    });
  }

  for (const value of lists.nestedChildren) {
    const normalized = Math.max(0, Math.floor(value));
    if (normalized === baseline.nestedChildren) {
      continue;
    }
    scenarios.push({
      ...baseline,
      nestedChildren: normalized,
      name: `nestedChildren=${normalized}`,
      factor: 'nestedChildren',
      factorValue: String(normalized),
    });
  }

  for (const value of lists.timerMs) {
    const normalized = Math.max(0, Math.floor(value));
    if (normalized === baseline.timerMs) {
      continue;
    }
    scenarios.push({
      ...baseline,
      timerMs: normalized,
      name: `timerMs=${normalized}`,
      factor: 'timerMs',
      factorValue: String(normalized),
    });
  }

  return scenarios;
};

const formatResults = (results: ScenarioResult[]): string => {
  const rows = [
    [
      'scenario',
      'factor',
      'value',
      'jobs',
      'steps',
      'payloadKB',
      'failRate',
      'nested',
      'timerMs',
      'avg ms',
      'p50 ms',
      'p95 ms',
      'fail %',
      'throughput j/s',
      'wall ms',
    ],
    ...results.map((item) => [
      item.config.name,
      item.config.factor,
      item.config.factorValue,
      String(item.config.jobCount),
      String(item.config.stepCount),
      String(item.config.payloadKb),
      item.config.failureRate.toFixed(2),
      String(item.config.nestedChildren),
      String(item.config.timerMs),
      item.avgLatencyMs.toFixed(2),
      item.p50LatencyMs.toFixed(2),
      item.p95LatencyMs.toFixed(2),
      ((item.failed / item.config.jobCount) * 100).toFixed(2),
      item.throughputJobsPerSec.toFixed(2),
      item.wallMs.toFixed(2),
    ]),
  ];

  const widths = rows[0].map((_value, columnIndex) =>
    Math.max(...rows.map((row) => row[columnIndex].length)),
  );

  return rows
    .map((row, rowIndex) => {
      const line = row
        .map((value, columnIndex) => value.padEnd(widths[columnIndex], ' '))
        .join(' | ');
      if (rowIndex === 0) {
        const separator = widths.map((width) => '-'.repeat(width)).join('-|-');
        return `${line}\n${separator}`;
      }
      return line;
    })
    .join('\n');
};

async function runScenario(params: {
  config: PerfScenarioConfig;
  queue: Queue<WorkflowJobData<unknown>>;
  engine: DozerEngine;
  workerConcurrency: number;
  producerConcurrency: number;
  completionConcurrency: number;
  timeoutMs: number;
  pollMs: number;
}): Promise<ScenarioResult> {
  const {
    config,
    queue,
    engine,
    workerConcurrency,
    producerConcurrency,
    completionConcurrency,
    timeoutMs,
    pollMs,
  } = params;

  await queue.obliterate({ force: true });

  const payload = buildPayload(config.payloadKb);
  const latencies: number[] = [];
  const statuses: Array<'completed' | 'failed'> = [];
  const scenarioWallStart = performance.now();

  // Prevent queue self-deadlock for nested workflow waits in the same queue.
  const safeBatchSize =
    config.nestedChildren > 0
      ? Math.max(1, workerConcurrency - 1)
      : config.jobCount;

  for (
    let offset = 0;
    offset < config.jobCount;
    offset += Math.max(1, safeBatchSize)
  ) {
    const batchCount = Math.min(safeBatchSize, config.jobCount - offset);
    const startedAt = new Map<string, number>();
    const jobIds = await mapWithConcurrency(
      batchCount,
      producerConcurrency,
      async (batchIndex) => {
        const index = offset + batchIndex;
        const input: PerfMainInput = {
          key: `${config.name}:job:${index}`,
          payload,
          stepCount: config.stepCount,
          nestedChildren: config.nestedChildren,
          timerMs: config.timerMs,
          failureRate: config.failureRate,
        };
        const enqueueStart = performance.now();
        const jobId = await engine.start('perf-main-workflow', input);
        startedAt.set(jobId, enqueueStart);
        return jobId;
      },
    );

    const terminals = await mapWithConcurrency(
      jobIds.length,
      completionConcurrency,
      async (index) => {
        const jobId = jobIds[index];
        const status = await waitForTerminal(queue, jobId, timeoutMs, pollMs);
        const endedAt = performance.now();
        return {
          jobId,
          status,
          endedAt,
        };
      },
    );

    for (const terminal of terminals) {
      const startedMs = startedAt.get(terminal.jobId) ?? terminal.endedAt;
      latencies.push(terminal.endedAt - startedMs);
      statuses.push(terminal.status);
    }
  }

  const wallMs = performance.now() - scenarioWallStart;
  const failed = statuses.filter((status) => status === 'failed').length;
  const completed = statuses.length - failed;
  const avgLatencyMs =
    latencies.length > 0
      ? latencies.reduce((sum, value) => sum + value, 0) / latencies.length
      : 0;

  return {
    config,
    completed,
    failed,
    avgLatencyMs,
    p50LatencyMs: percentile(latencies, 50),
    p95LatencyMs: percentile(latencies, 95),
    throughputJobsPerSec:
      wallMs > 0 ? statuses.length / (wallMs / 1000) : 0,
    wallMs,
  };
}

async function main(): Promise<void> {
  const workerConcurrency = intArg('worker-concurrency', 20);
  const producerConcurrency = intArg('producer-concurrency', 50);
  const completionConcurrency = intArg('completion-concurrency', 100);
  const timeoutMs = intArg('timeout-ms', 120000);
  const pollMs = intArg('poll-ms', 20);
  const matrixMode = boolArg('matrix', false);

  const baseline = {
    stepCount: intArg('baseline-steps', 5),
    payloadKb: intArg('baseline-payload-kb', 4),
    jobCount: intArg('baseline-jobs', 50),
    failureRate: floatArg('baseline-failure-rate', 0),
    nestedChildren: intArg('baseline-nested', 0),
    timerMs: intArg('baseline-timer-ms', 0),
  };

  const lists = {
    steps: listArg('steps', [3, 8, 16]),
    payloadKb: listArg('payload-kb', [1, 16, 64]),
    jobs: listArg('jobs', [25, 100, 300]),
    failureRate: listArg('failure-rate', [0, 0.1, 0.3]),
    nestedChildren: listArg('nested-children', [0, 2, 5]),
    timerMs: listArg('timer-ms', [0, 5, 20]),
  };

  let scenarios = buildSweepScenarios(baseline, lists);
  if (matrixMode) {
    scenarios = [];
    for (const stepCount of lists.steps) {
      for (const payloadKb of lists.payloadKb) {
        for (const jobCount of lists.jobs) {
          for (const failureRate of lists.failureRate) {
            for (const nestedChildren of lists.nestedChildren) {
              for (const timerMs of lists.timerMs) {
                scenarios.push({
                  name: `m:st${stepCount}:pl${payloadKb}:j${jobCount}:fr${failureRate}:n${nestedChildren}:t${timerMs}`,
                  factor: 'matrix',
                  factorValue: '-',
                  stepCount: Math.max(1, Math.floor(stepCount)),
                  payloadKb: Math.max(1, Math.floor(payloadKb)),
                  jobCount: Math.max(1, Math.floor(jobCount)),
                  failureRate: Math.min(1, Math.max(0, Number(failureRate))),
                  nestedChildren: Math.max(0, Math.floor(nestedChildren)),
                  timerMs: Math.max(0, Math.floor(timerMs)),
                });
              }
            }
          }
        }
      }
    }
  }

  process.stdout.write(
    `Running real-Redis perf scenarios: ${scenarios.length}\n`,
  );
  process.stdout.write(
    `worker=${workerConcurrency}, producer=${producerConcurrency}, completion=${completionConcurrency}, timeoutMs=${timeoutMs}\n`,
  );

  const moduleRef: TestingModule = await Test.createTestingModule({
    imports: [PerfModule],
  }).compile();
  await moduleRef.init();

  let worker: Worker | undefined;
  let queue: Queue<WorkflowJobData<unknown>> | undefined;
  let connection: ConnectionOptions | undefined;

  try {
    queue = moduleRef.get<Queue<WorkflowJobData<unknown>>>(
      EXAMPLE_WORKFLOW_QUEUE,
    );
    const engine = moduleRef.get(DozerEngine);
    connection = moduleRef.get<ConnectionOptions>(
      EXAMPLE_REDIS_CONNECTION,
    );

    worker = new Worker(
      queue.name,
      async (job) => {
        await engine.run(String(job.id));
      },
      {
        connection,
        concurrency: workerConcurrency,
      },
    );

    const results: ScenarioResult[] = [];
    for (let index = 0; index < scenarios.length; index += 1) {
      const scenario = scenarios[index];
      process.stdout.write(
        `[${index + 1}/${scenarios.length}] ${scenario.name} (steps=${scenario.stepCount}, payloadKB=${scenario.payloadKb}, jobs=${scenario.jobCount}, failureRate=${scenario.failureRate}, nested=${scenario.nestedChildren}, timerMs=${scenario.timerMs})\n`,
      );
      const result = await runScenario({
        config: scenario,
        queue,
        engine,
        workerConcurrency,
        producerConcurrency,
        completionConcurrency,
        timeoutMs,
        pollMs,
      });
      results.push(result);
    }

    process.stdout.write('\n');
    process.stdout.write(`${formatResults(results)}\n`);
  } finally {
    if (worker) {
      await worker.close();
    }
    if (queue) {
      await queue.close().catch(() => {
        // Queue might already be closed by module shutdown.
      });
    }
    if (connection && isRedisClientConnection(connection)) {
      await disconnectRedisClient(connection);
    }
    await moduleRef.close();
  }
}

main().catch((error: unknown) => {
  process.stderr.write(`Perf runner failed: ${String(error)}\n`);
  process.exitCode = 1;
});
