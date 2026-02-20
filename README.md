# dozer

`dozer` is a NestJS module for deterministic workflow orchestration on top of BullMQ.

It is built for Temporal-like workflow semantics with familiar NestJS patterns:
- workflows are regular DI classes
- each workflow has a single `run(input)` method
- steps are explicit methods marked with `@Step()`
- completed steps are replayed from cache, not re-executed

## Features

- ✅ Deterministic Replay: workflows resume from cached step state and detect replay divergence
- ✅ Step Caching: completed step results are persisted in BullMQ/Redis job data
- ✅ Distributed Execution: built on BullMQ workers for distributed processing
- ✅ Redis Cluster Support: `example` infrastructure supports standalone Redis and Redis Cluster
- ✅ Nested Steps: supports nested step indexing (`0:*`, `0.0:*`, ...)
- ✅ Retry Strategies: `constant`, `linear`, `exponential` backoff + step timeouts
- ✅ Error Handling: retryable/non-retryable (`NonRetryableError`) and deterministic failure states
- ✅ Type-Safe: TypeScript-first API and typed workflow decorators
- ✅ Production-Oriented: recovery/replay patterns covered by unit/integration/e2e/perf scenarios
- ✅ Priority Queues: BullMQ job options (including priority) are supported via workflow/module defaults

## Installation

```bash
npm install dozer
```

Peer dependencies:
- `@nestjs/common`
- `@nestjs/core`
- `reflect-metadata`
- `rxjs`

## Quick Start

### 1) Register module and workflows

```ts
import { Module } from '@nestjs/common';
import { Queue } from 'bullmq';
import { DozerModule } from 'dozer';
import { OrderWorkflow } from './order.workflow';

const queue = new Queue('wf', {
  connection: { host: '127.0.0.1', port: 6379 },
});

@Module({
  imports: [
    DozerModule.forRoot({
      queue,
      defaults: {
        execution: {
          stepRetry: { attempts: 2, delayMs: 100, strategy: 'linear' },
          workflowRetry: {
            attempts: 3,
            delayMs: 300,
            strategy: 'exponential',
          },
        },
      },
    }),
    DozerModule.forFeature([OrderWorkflow]),
  ],
})
export class AppModule {}
```

For Redis Cluster:

```ts
import Redis from 'ioredis';
import { Queue } from 'bullmq';

const cluster = new Redis.Cluster([
  { host: '10.0.0.11', port: 6379 },
  { host: '10.0.0.12', port: 6379 },
  { host: '10.0.0.13', port: 6379 },
]);

const queue = new Queue('wf', {
  connection: cluster,
});
```

### 2) Define a workflow

```ts
import { Step, Workflow } from 'dozer';

@Workflow({ name: 'order-flow' })
export class OrderWorkflow {
  @Step()
  async validate(input: { orderId: number }) {
    return { ...input, validated: true };
  }

  @Step({ retry: { attempts: 3, delayMs: 200, strategy: 'constant' } })
  async process(input: { orderId: number; validated: boolean }) {
    return { ...input, processed: true };
  }

  @Step({ timeout: 5_000 })
  async store(input: { orderId: number; validated: boolean; processed: boolean }) {
    return { ...input, stored: true };
  }

  async run(input: { orderId: number }) {
    const validated = await this.validate(input);
    const processed = await this.process(validated);
    const stored = await this.store(processed);
    return { ok: true, payload: stored };
  }
}
```

### 3) Start workflow

```ts
import { Injectable } from '@nestjs/common';
import { DozerEngine } from 'dozer';

@Injectable()
export class OrdersService {
  constructor(private readonly engine: DozerEngine) {}

  async start(orderId: number) {
    return this.engine.start('order-flow', { orderId });
  }
}
```

### 4) Run worker loop

`engine.start()` only enqueues a job. You still need a BullMQ worker process:

```ts
import { Worker } from 'bullmq';
import { DozerEngine } from 'dozer';

export const createWorkflowWorker = (engine: DozerEngine) =>
  new Worker(
    'wf',
    async (job) => {
      await engine.run(String(job.id));
    },
    { connection: { host: '127.0.0.1', port: 6379 } },
  );
```

## Client-only Mode

If another service only starts workflows and does not host workers:

```ts
DozerModule.forClient({
  queue,
});
```

Use `DozerClient` or `DozerEngine.start(...)` from that service.

## Decorator Options

### `@Workflow(options)`

- `name: string` (required)
- `job?: WorkflowJobOptions` (BullMQ job options defaults for this workflow)
- `execution?: WorkflowExecutionOptions`
  - `stepRetry?: RetryOptions`
  - `workflowRetry?: RetryOptions`
  - `autoDeterminismProbe?: boolean`
  - `determinismProbeMaxDurationMs?: number`

### `@Step(options)`

- `name?: string`
- `retry?: RetryOptions`
  - `attempts?: number`
  - `delayMs?: number`
  - `strategy?: 'constant' | 'linear' | 'exponential'`
- `timeout?: number` (ms)
- `timeoutMs?: number` (alias)

`StepOptions.queue` is intentionally not part of the API.

## Error Model

- `WorkflowNotRegisteredError`: workflow name is missing in DI registry
- `WorkflowJobNotFoundError`: job id does not exist
- `StepReplayConflictError`: replay step sequence diverged
- `NonDeterminismError`: determinism validation failed
- `SerializationError`: unsupported serialized value
- `NonRetryableError`: mark step failure as terminal for retries
- `TimeoutError`: step execution exceeded configured timeout

## Compact State Format

State is stored in `job.data` with short keys to minimize overhead:

- `i`: serialized workflow input
- `d.s`: status code (`pending`, `running`, `failed`, `completed`)
- `d.c`: cached step results
- `d.u`: keys for steps that returned `undefined`
- `d.a`: per-step retry attempts
- `d.t`: full trace of visited step keys
- `d.r`: final serialized workflow result
- `d.e`: error message

When a parent step result is saved, descendant cache keys are removed from `d.c`, `d.u`, and `d.a` (trace `d.t` is preserved).

## Serialization Support

Supported values include:
- JSON primitives/objects/arrays
- `Date`
- `Buffer`
- `Uint8Array` and other typed arrays
- `ArrayBuffer`
- `Blob`

Unsupported values fail with `SerializationError` (for example: functions, symbols, bigint, circular references).

## Repository Layout

- `src/` - dozer library
- `example/` - runnable NestJS app with BullMQ, Redis, Bull Board, integration/e2e/perf tests
- `TEST_MATRIX.md` - behavior contract covered by tests
- `IMPLEMENTATION_PLAN.md` - implementation checklist and status

## Development Commands

From repository root:

```bash
npm run lint
npm run test:unit
npm run test:integration
npm run test:e2e
npm run test:all
npm run build
npm run bench
```

Example app:

```bash
cd example
npm install
npm run redis:up
npm run test
npm run perf
npm run start:dev
```

Redis connection env (standalone):
- `REDIS_HOST=127.0.0.1`
- `REDIS_PORT=6379`

Redis Cluster env:
- `REDIS_MODE=cluster` (or `REDIS_CLUSTER=1`)
- `REDIS_CLUSTER_NODES=10.0.0.11:6379,10.0.0.12:6379,10.0.0.13:6379`

Bull Board is available at `http://localhost:3100/admin/queues/`.

## Publishing

Dry-run package validation:

```bash
npm pack --dry-run
```

Publish to npm:

```bash
npm publish --access public
```

`prepack` runs `npm run build`, so compiled artifacts are always generated before publish.

## License

[MIT](./LICENSE)

Contributions: [CONTRIBUTING.md](./CONTRIBUTING.md)
