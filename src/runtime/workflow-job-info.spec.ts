import { DOZER_JOB_INPUT_KEY, DOZER_JOB_STATE_KEY } from '../constants';
import type { CompactWorkflowState, WorkflowJob } from '../queue/workflow-queue';
import { WORKFLOW_STATUS } from '../queue/workflow-queue';
import { getWorkflowJobInfo } from './workflow-job-info';

const makeJob = (state: CompactWorkflowState | undefined): WorkflowJob<unknown> => ({
  id: 'job-1',
  name: 'my-workflow',
  data: {
    [DOZER_JOB_INPUT_KEY]: {},
    [DOZER_JOB_STATE_KEY]: state,
  },
  updateData: async () => {},
});

const validState = (overrides: Partial<CompactWorkflowState> = {}): CompactWorkflowState => ({
  s: WORKFLOW_STATUS.pending,
  c: {},
  t: [],
  ...overrides,
});

describe('getWorkflowJobInfo', () => {
  it('returns id and name from job', () => {
    const info = getWorkflowJobInfo(makeJob(validState()));
    expect(info.id).toBe('job-1');
    expect(info.name).toBe('my-workflow');
  });

  it('falls back to pending state when state is undefined', () => {
    const info = getWorkflowJobInfo(makeJob(undefined));
    expect(info.status).toBe(WORKFLOW_STATUS.pending);
    expect(info.statusName).toBe('pending');
  });

  it('falls back to pending state when state has no status code', () => {
    const info = getWorkflowJobInfo(makeJob({ s: 99 as never, c: {}, t: [] }));
    expect(info.status).toBe(WORKFLOW_STATUS.pending);
    expect(info.statusName).toBe('pending');
  });

  it('falls back when state is not an object', () => {
    const job: WorkflowJob<unknown> = {
      id: 'j',
      name: 'w',
      data: {
        [DOZER_JOB_INPUT_KEY]: {},
        [DOZER_JOB_STATE_KEY]: 'corrupted' as never,
      },
      updateData: async () => {},
    };
    expect(getWorkflowJobInfo(job).status).toBe(WORKFLOW_STATUS.pending);
  });

  describe('statusName mapping', () => {
    it.each([
      [WORKFLOW_STATUS.pending, 'pending'],
      [WORKFLOW_STATUS.running, 'running'],
      [WORKFLOW_STATUS.failed, 'failed'],
      [WORKFLOW_STATUS.completed, 'completed'],
      [WORKFLOW_STATUS.cancelled, 'cancelled'],
      [WORKFLOW_STATUS.completing, 'completing'],
    ] as const)('status %i maps to statusName "%s"', (code, name) => {
      const info = getWorkflowJobInfo(makeJob(validState({ s: code })));
      expect(info.statusName).toBe(name);
    });
  });

  describe('result field', () => {
    it('is undefined for non-completed statuses', () => {
      for (const s of [
        WORKFLOW_STATUS.pending,
        WORKFLOW_STATUS.running,
        WORKFLOW_STATUS.failed,
        WORKFLOW_STATUS.cancelled,
      ] as const) {
        const info = getWorkflowJobInfo(makeJob(validState({ s, r: 'ignored' })));
        expect(info.result).toBeUndefined();
      }
    });

    it('is present and deserialized when status is completed', () => {
      const info = getWorkflowJobInfo(
        makeJob(validState({ s: WORKFLOW_STATUS.completed, r: 42 })),
      );
      expect(info.result).toBe(42);
    });

    it('is present when status is completing', () => {
      const info = getWorkflowJobInfo(
        makeJob(validState({ s: WORKFLOW_STATUS.completing, r: 'done' })),
      );
      expect(info.result).toBe('done');
    });

    it('is undefined when completed but no r field in state', () => {
      const info = getWorkflowJobInfo(
        makeJob(validState({ s: WORKFLOW_STATUS.completed })),
      );
      expect(info.result).toBeUndefined();
    });
  });

  describe('error field', () => {
    it('is undefined for non-failed statuses', () => {
      for (const s of [
        WORKFLOW_STATUS.pending,
        WORKFLOW_STATUS.running,
        WORKFLOW_STATUS.completed,
        WORKFLOW_STATUS.completing,
      ] as const) {
        const info = getWorkflowJobInfo(makeJob(validState({ s, e: 'ignored' })));
        expect(info.error).toBeUndefined();
      }
    });

    it('is present when status is failed and e is set', () => {
      const info = getWorkflowJobInfo(
        makeJob(validState({ s: WORKFLOW_STATUS.failed, e: 'some error' })),
      );
      expect(info.error).toBe('some error');
    });

    it('is present when status is cancelled and e is set', () => {
      const info = getWorkflowJobInfo(
        makeJob(validState({ s: WORKFLOW_STATUS.cancelled, e: 'cancelled reason' })),
      );
      expect(info.error).toBe('cancelled reason');
    });

    it('is undefined when failed but e field missing', () => {
      const info = getWorkflowJobInfo(
        makeJob(validState({ s: WORKFLOW_STATUS.failed })),
      );
      expect(info.error).toBeUndefined();
    });
  });
});
