import { WorkflowNotRegisteredError } from '../errors/workflow-not-registered.error';
import type {
  RegisteredWorkflowOptions,
  RunnableWorkflow,
} from './workflow-registry';
import { WorkflowRegistry } from './workflow-registry';

class FakeWorkflow {
  run(): Promise<unknown> {
    return Promise.resolve('result');
  }
}

class AnotherWorkflow {
  run(): Promise<unknown> {
    return Promise.resolve('other');
  }
}

class NoRunWorkflow {}

const makeFactory = (instance: RunnableWorkflow) => () => instance;

describe('WorkflowRegistry', () => {
  let registry: WorkflowRegistry;

  beforeEach(() => {
    registry = new WorkflowRegistry();
  });

  describe('register()', () => {
    it('registers a workflow and allows resolution', () => {
      const instance = new FakeWorkflow();
      registry.register('my-workflow', FakeWorkflow, makeFactory(instance));
      expect(() => registry.resolveDefinition('my-workflow')).not.toThrow();
    });

    it('allows re-registering same class with same name (idempotent)', () => {
      const instance = new FakeWorkflow();
      const factory = makeFactory(instance);
      registry.register('my-workflow', FakeWorkflow, factory);
      expect(() =>
        registry.register('my-workflow', FakeWorkflow, factory),
      ).not.toThrow();
    });

    it('throws when registering same name with different class', () => {
      registry.register(
        'my-workflow',
        FakeWorkflow,
        makeFactory(new FakeWorkflow()),
      );
      expect(() =>
        registry.register(
          'my-workflow',
          AnotherWorkflow,
          makeFactory(new AnotherWorkflow()),
        ),
      ).toThrow('already registered with another instance');
    });

    it('stores provided options on the definition', () => {
      const opts: RegisteredWorkflowOptions = {
        job: { attempts: 5 },
      };
      registry.register(
        'my-workflow',
        FakeWorkflow,
        makeFactory(new FakeWorkflow()),
        opts,
      );
      const def = registry.resolveDefinition('my-workflow');
      expect(def.options).toEqual(opts);
    });

    it('defaults options to empty object when not provided', () => {
      registry.register(
        'my-workflow',
        FakeWorkflow,
        makeFactory(new FakeWorkflow()),
      );
      const def = registry.resolveDefinition('my-workflow');
      expect(def.options).toEqual({});
    });
  });

  describe('resolveDefinition()', () => {
    it('throws WorkflowNotRegisteredError for unknown workflow', () => {
      expect(() => registry.resolveDefinition('unknown')).toThrow(
        WorkflowNotRegisteredError,
      );
    });

    it('error message includes workflow name', () => {
      expect(() => registry.resolveDefinition('missing-wf')).toThrow(
        '"missing-wf"',
      );
    });

    it('throws when class prototype has no run() method', () => {
      registry.register(
        'bad-workflow',
        NoRunWorkflow as never,
        makeFactory({ run: async () => {} }),
      );
      expect(() => registry.resolveDefinition('bad-workflow')).toThrow(
        'must expose "run(input)" method',
      );
    });
  });

  describe('resolve()', () => {
    it('returns the instance produced by factory', () => {
      const instance = new FakeWorkflow();
      registry.register('my-workflow', FakeWorkflow, makeFactory(instance));
      expect(registry.resolve('my-workflow')).toBe(instance);
    });

    it('throws when factory returns object without run()', () => {
      registry.register('my-workflow', FakeWorkflow, () => ({}) as never);
      expect(() => registry.resolve('my-workflow')).toThrow(
        'must expose "run(input)" method',
      );
    });
  });

  describe('resolveOptionalDefinition()', () => {
    it('returns null for unknown workflow', () => {
      expect(registry.resolveOptionalDefinition('nope')).toBeNull();
    });

    it('returns definition for registered workflow', () => {
      registry.register(
        'my-workflow',
        FakeWorkflow,
        makeFactory(new FakeWorkflow()),
      );
      expect(registry.resolveOptionalDefinition('my-workflow')).not.toBeNull();
    });
  });
});
