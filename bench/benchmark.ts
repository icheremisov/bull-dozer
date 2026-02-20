import { performance } from 'node:perf_hooks';
import { Injectable } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import {
  DozerEngine,
  DozerModule,
  InMemoryWorkflowQueue,
  Step,
  Workflow,
} from '../src';

@Injectable()
class BenchFailOnceService {
  private readonly attempts = new Map<string, number>();

  shouldFail(key: string, failTimes = 1): boolean {
    const current = this.attempts.get(key) ?? 0;
    this.attempts.set(key, current + 1);
    return current < failTimes;
  }
}

@Workflow({ name: 'bench-single-step' })
class BenchSingleStepWorkflow {
  @Step({ name: 'inc' })
  inc(value: number): Promise<number> {
    return Promise.resolve(value + 1);
  }

  run(input: { value: number }): Promise<number> {
    return this.inc(input.value);
  }
}

@Workflow({ name: 'bench-multi-step' })
class BenchMultiStepWorkflow {
  @Step({ name: 'validate' })
  validate(value: number): Promise<number> {
    return Promise.resolve(value);
  }

  @Step({ name: 'transform' })
  transform(value: number): Promise<number> {
    return Promise.resolve(value * 2);
  }

  @Step({ name: 'pack' })
  pack(value: number): Promise<{ value: number }> {
    return Promise.resolve({ value });
  }

  async run(input: { value: number }): Promise<{ value: number }> {
    const validated = await this.validate(input.value);
    const transformed = await this.transform(validated);
    return this.pack(transformed);
  }
}

@Workflow({ name: 'bench-replay-resume' })
class BenchReplayResumeWorkflow {
  constructor(private readonly failOnce: BenchFailOnceService) {}

  @Step({ name: 'prepare' })
  prepare(value: number): Promise<number> {
    return Promise.resolve(value + 1);
  }

  @Step({ name: 'fail-once' })
  failOnOnce(id: string): Promise<void> {
    if (this.failOnce.shouldFail(`bench-replay:${id}`, 1)) {
      return Promise.reject(new Error('bench-fail-once'));
    }

    return Promise.resolve();
  }

  async run(input: { id: string; value: number }): Promise<{ value: number }> {
    const prepared = await this.prepare(input.value);
    await this.failOnOnce(input.id);
    return { value: prepared };
  }
}

interface BenchCase {
  name: string;
  runIteration: (iteration: number) => Promise<number>;
}

interface BenchResult {
  name: string;
  iterations: number;
  totalMs: number;
  opsPerSec: number;
  meanMs: number;
  p50Ms: number;
  p95Ms: number;
}

const parseIntArg = (name: string, fallback: number): number => {
  const envName = `BENCH_${name.toUpperCase()}`;
  const envValue = process.env[envName];
  const prefixedArg = process.argv.find((arg) =>
    arg.startsWith(`--${name}=`),
  );
  const splitValue = prefixedArg?.split('=')[1];
  const directIndex = process.argv.indexOf(`--${name}`);
  const directValue =
    directIndex >= 0 ? process.argv[directIndex + 1] : undefined;
  const raw = splitValue ?? directValue ?? envValue;
  const parsed = raw ? Number.parseInt(raw, 10) : fallback;

  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }

  return parsed;
};

const percentile = (samples: number[], p: number): number => {
  if (samples.length === 0) {
    return 0;
  }

  const sorted = [...samples].sort((a, b) => a - b);
  const rank = Math.ceil((p / 100) * sorted.length) - 1;
  const index = Math.min(sorted.length - 1, Math.max(0, rank));
  return sorted[index];
};

const measureAsync = async (fn: () => Promise<void>): Promise<number> => {
  const startedAt = performance.now();
  await fn();
  return performance.now() - startedAt;
};

const runCase = async (
  scenario: BenchCase,
  iterations: number,
  warmup: number,
): Promise<BenchResult> => {
  for (let i = 0; i < warmup; i += 1) {
    await scenario.runIteration(i - warmup);
  }

  const samples: number[] = [];
  let totalMs = 0;

  for (let i = 0; i < iterations; i += 1) {
    const durationMs = await scenario.runIteration(i);
    samples.push(durationMs);
    totalMs += durationMs;
  }

  return {
    name: scenario.name,
    iterations,
    totalMs,
    opsPerSec: (iterations * 1000) / totalMs,
    meanMs: totalMs / iterations,
    p50Ms: percentile(samples, 50),
    p95Ms: percentile(samples, 95),
  };
};

const formatTable = (results: BenchResult[]): string => {
  const rows = [
    [
      'scenario',
      'iterations',
      'ops/s',
      'mean ms',
      'p50 ms',
      'p95 ms',
      'total ms',
    ],
    ...results.map((result) => [
      result.name,
      String(result.iterations),
      result.opsPerSec.toFixed(2),
      result.meanMs.toFixed(4),
      result.p50Ms.toFixed(4),
      result.p95Ms.toFixed(4),
      result.totalMs.toFixed(2),
    ]),
  ];

  const widths = rows[0].map((_cell, index) =>
    Math.max(...rows.map((row) => row[index].length)),
  );

  return rows
    .map((row, rowIndex) => {
      const line = row
        .map((cell, cellIndex) => cell.padEnd(widths[cellIndex], ' '))
        .join(' | ');
      if (rowIndex === 0) {
        const separator = widths.map((width) => '-'.repeat(width)).join('-|-');
        return `${line}\n${separator}`;
      }
      return line;
    })
    .join('\n');
};

const createCases = (engine: DozerEngine): BenchCase[] => [
  {
    name: 'single-step start+run',
    runIteration: (iteration) =>
      measureAsync(async () => {
        const jobId = await engine.start('bench-single-step', {
          value: iteration,
        });
        await engine.run(jobId);
      }),
  },
  {
    name: 'multi-step start+run',
    runIteration: (iteration) =>
      measureAsync(async () => {
        const jobId = await engine.start('bench-multi-step', {
          value: iteration,
        });
        await engine.run(jobId);
      }),
  },
  {
    name: 'resume-only run (after fail)',
    runIteration: async (iteration) => {
      const jobId = await engine.start('bench-replay-resume', {
        id: `resume-${iteration}`,
        value: iteration,
      });

      try {
        await engine.run(jobId);
        throw new Error('Benchmark setup failed: first run should fail once.');
      } catch (error) {
        if (!(error instanceof Error) || error.message !== 'bench-fail-once') {
          throw error;
        }
      }

      return measureAsync(async () => {
        await engine.run(jobId);
      });
    },
  },
];

async function main(): Promise<void> {
  const iterations = parseIntArg('iterations', 250);
  const warmup = parseIntArg('warmup', 25);

  const queue = new InMemoryWorkflowQueue();
  const moduleRef: TestingModule = await Test.createTestingModule({
    imports: [
      DozerModule.forRoot({
        driver: queue,
      }),
      DozerModule.forFeature(
        [
          BenchSingleStepWorkflow,
          BenchMultiStepWorkflow,
          BenchReplayResumeWorkflow,
        ],
        [BenchFailOnceService],
      ),
    ],
  }).compile();

  await moduleRef.init();

  try {
    const engine = moduleRef.get(DozerEngine);
    const cases = createCases(engine);
    const results: BenchResult[] = [];

    process.stdout.write(
      `Running dozer benchmark: iterations=${iterations}, warmup=${warmup}\n`,
    );

    for (const scenario of cases) {
      process.stdout.write(`- ${scenario.name}\n`);
      results.push(await runCase(scenario, iterations, warmup));
    }

    process.stdout.write('\n');
    process.stdout.write(`${formatTable(results)}\n`);
  } finally {
    await moduleRef.close();
  }
}

main().catch((error: unknown) => {
  process.stderr.write(`Benchmark failed: ${String(error)}\n`);
  process.exitCode = 1;
});
