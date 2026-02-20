# Real Redis Performance Runner

`example/scripts/perf-runner.ts` runs load scenarios on a real Redis + BullMQ setup and prints a latency/throughput table.

By default it uses a dedicated queue (`wf-perf`) so performance runs do not interfere with the regular application queue (`wf`).

## What it measures

- step count impact (`stepCount`)
- payload size impact (`payloadKb`)
- number of jobs (`jobCount`)
- unstable workflows with retry (`failureRate`)
- nested workflow fan-out (`nestedChildren`)
- timers inside steps (`timerMs`)

For scenarios with `nestedChildren > 0`, the runner automatically batches execution below worker concurrency to avoid parent/child deadlocks on the same queue.

## Commands

```bash
npm run redis:up
npm run perf
npm run perf:quick
```

Override queue name if needed:

```bash
PERF_WORKFLOW_QUEUE_NAME=my-perf-queue npm run perf
```

Redis Cluster can be used by setting:

```bash
REDIS_MODE=cluster
REDIS_CLUSTER_NODES=10.0.0.11:6379,10.0.0.12:6379,10.0.0.13:6379
```

## Main CLI arguments

Baseline:
- `--baseline-steps`
- `--baseline-payload-kb`
- `--baseline-jobs`
- `--baseline-failure-rate`
- `--baseline-nested`
- `--baseline-timer-ms`

Sweep lists:
- `--steps=3,8,16`
- `--payload-kb=1,16,64`
- `--jobs=25,100,300`
- `--failure-rate=0,0.1,0.3`
- `--nested-children=0,2,5`
- `--timer-ms=0,5,20`

Execution tuning:
- `--worker-concurrency=20`
- `--producer-concurrency=50`
- `--completion-concurrency=100`
- `--timeout-ms=120000`
- `--poll-ms=20`
- `--matrix=true` (full cartesian product for all lists)

## Example

```bash
npm run perf -- --baseline-jobs=200 --steps=5,10,20 --payload-kb=4,32
```

## Output columns

For each scenario:
- `avg ms`, `p50 ms`, `p95 ms`
- `fail %`
- `throughput j/s`
- `wall ms`
