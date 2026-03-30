import { Test, TestingModule } from '@nestjs/testing';
import {
  DozerEngine,
  DozerModule,
  DozerWorkflow,
  InMemoryWorkflowQueue,
  Step,
  Workflow,
} from './index';

// ---------------------------------------------------------------------------
// Shared input type
// ---------------------------------------------------------------------------

type JobMethodsInput = {
  logs?: string[];
  progress?: number | object;
  priority?: number;
  clearAfter?: number;
};

// ---------------------------------------------------------------------------
// Workflow fixtures
// ---------------------------------------------------------------------------

@Workflow({ name: 'job-methods-workflow' })
class JobMethodsWorkflow extends DozerWorkflow<JobMethodsInput> {
  @Step({ name: 'execute' })
  async execute(input: JobMethodsInput): Promise<void> {
    for (const row of input.logs ?? []) {
      await this.log(row);
    }
    if (input.clearAfter !== undefined) {
      await this.clearLogs(input.clearAfter);
    }
    if (input.progress !== undefined) {
      await this.updateProgress(input.progress);
    }
    if (input.priority !== undefined) {
      await this.changePriority({ priority: input.priority });
    }
  }

  async run(input: JobMethodsInput): Promise<void> {
    await this.execute(input);
  }
}

// keepLogs variants — keepLogs is baked into the @Workflow decorator
@Workflow({ name: 'log-keep-none' })
class LogKeepNoneWorkflow extends DozerWorkflow<string[]> {
  @Step({ name: 'log-all' })
  async logAll(entries: string[]): Promise<void> {
    for (const row of entries) {
      await this.log(row);
    }
  }

  async run(input: string[]): Promise<void> {
    await this.logAll(input);
  }
}

@Workflow({ name: 'log-keep-0', job: { keepLogs: 0 } })
class LogKeep0Workflow extends DozerWorkflow<string[]> {
  @Step({ name: 'log-all' })
  async logAll(entries: string[]): Promise<void> {
    for (const row of entries) {
      await this.log(row);
    }
  }

  async run(input: string[]): Promise<void> {
    await this.logAll(input);
  }
}

@Workflow({ name: 'log-keep-5', job: { keepLogs: 5 } })
class LogKeep5Workflow extends DozerWorkflow<string[]> {
  @Step({ name: 'log-all' })
  async logAll(entries: string[]): Promise<void> {
    for (const row of entries) {
      await this.log(row);
    }
  }

  async run(input: string[]): Promise<void> {
    await this.logAll(input);
  }
}

@Workflow({ name: 'log-keep-3', job: { keepLogs: 3 } })
class LogKeep3Workflow extends DozerWorkflow<string[]> {
  @Step({ name: 'log-all' })
  async logAll(entries: string[]): Promise<void> {
    for (const row of entries) {
      await this.log(row);
    }
  }

  async run(input: string[]): Promise<void> {
    await this.logAll(input);
  }
}

// ---------------------------------------------------------------------------
// Module setup
// ---------------------------------------------------------------------------

describe('DozerEngine job runtime methods', () => {
  let moduleRef: TestingModule;
  let queue: InMemoryWorkflowQueue;
  let engine: DozerEngine;

  beforeEach(async () => {
    queue = new InMemoryWorkflowQueue();
    moduleRef = await Test.createTestingModule({
      imports: [
        DozerModule.forRoot({ driver: queue }),
        DozerModule.forFeature([
          JobMethodsWorkflow,
          LogKeepNoneWorkflow,
          LogKeep0Workflow,
          LogKeep5Workflow,
          LogKeep3Workflow,
        ]),
      ],
    }).compile();
    await moduleRef.init();
    engine = moduleRef.get(DozerEngine);
  });

  afterEach(async () => {
    await moduleRef?.close();
  });

  // -------------------------------------------------------------------------
  // log() / clearLogs()
  // -------------------------------------------------------------------------

  describe('log()', () => {
    it('records entries in order', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        logs: ['alpha', 'beta', 'gamma'],
      });
      await engine.run(jobId);

      const { logs, count } = await queue.getJobLogs(jobId);
      expect(logs).toEqual(['alpha', 'beta', 'gamma']);
      expect(count).toBe(3);
    });

    it('returns the running entry count via getJobLogs().count', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        logs: ['a', 'b'],
      });
      await engine.run(jobId);

      const { count } = await queue.getJobLogs(jobId);
      expect(count).toBe(2);
    });
  });

  describe('clearLogs()', () => {
    it('clears all entries when called with keepLast 0', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        logs: ['x', 'y', 'z'],
        clearAfter: 0,
      });
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toEqual([]);
    });

    it('retains the last N entries when called with keepLast', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        logs: ['a', 'b', 'c', 'd', 'e'],
        clearAfter: 2,
      });
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toEqual(['d', 'e']);
    });

    it('retains all entries when keepLast exceeds actual count', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        logs: ['a', 'b'],
        clearAfter: 10,
      });
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toEqual(['a', 'b']);
    });
  });

  // -------------------------------------------------------------------------
  // updateProgress() / progress getter
  // -------------------------------------------------------------------------

  describe('updateProgress()', () => {
    it('sets numeric progress on the job', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        progress: 50,
      });
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.progress).toBe(50);
    });

    it('sets object progress on the job', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        progress: { step: 'uploading', pct: 30 },
      });
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.progress).toEqual({ step: 'uploading', pct: 30 });
    });

    it('defaults to 0 before any updateProgress call', async () => {
      const jobId = await engine.start('job-methods-workflow', {});
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.progress).toBe(0);
    });

    it('reflects the last updateProgress value', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        progress: 99,
      });
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.progress).toBe(99);
    });
  });

  // -------------------------------------------------------------------------
  // changePriority() / priority getter
  // -------------------------------------------------------------------------

  describe('changePriority()', () => {
    it('defaults to 0 before any changePriority call', async () => {
      const jobId = await engine.start('job-methods-workflow', {});
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.priority).toBe(0);
    });

    it('updates priority on the job', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        priority: 5,
      });
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.priority).toBe(5);
    });

    it('can reset priority back to 0', async () => {
      const jobId = await engine.start('job-methods-workflow', {
        priority: 0,
      });
      await engine.run(jobId);

      const job = await queue.get(jobId);
      expect(job?.priority).toBe(0);
    });
  });

  // -------------------------------------------------------------------------
  // keepLogs job option
  // -------------------------------------------------------------------------

  describe('keepLogs job option', () => {
    const TEN_ENTRIES = Array.from({ length: 10 }, (_, i) => `entry-${i}`);

    it('keeps all entries when keepLogs is undefined', async () => {
      const jobId = await engine.start('log-keep-none', TEN_ENTRIES);
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toHaveLength(10);
      expect(logs[0]).toBe('entry-0');
      expect(logs[9]).toBe('entry-9');
    });

    it('keeps all entries when keepLogs is 0 (treated as unlimited)', async () => {
      const jobId = await engine.start('log-keep-0', TEN_ENTRIES);
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toHaveLength(10);
    });

    it('trims to last N entries when keepLogs is 5', async () => {
      const jobId = await engine.start('log-keep-5', TEN_ENTRIES);
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toHaveLength(5);
      expect(logs[0]).toBe('entry-5');
      expect(logs[4]).toBe('entry-9');
    });

    it('keeps all entries when count equals keepLogs (boundary: count === keepLogs)', async () => {
      const jobId = await engine.start('log-keep-3', ['a', 'b', 'c']);
      await engine.run(jobId);

      const { logs } = await queue.getJobLogs(jobId);
      expect(logs).toEqual(['a', 'b', 'c']);
    });
  });
});
