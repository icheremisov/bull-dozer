import 'reflect-metadata';
import { Step } from './step.decorator';
import { NoStep } from './no-step.decorator';
import { DozerWorkflow } from '../workflow/dozer-workflow';

describe('@Workflow validation', () => {
  it('throws when class does not extend DozerWorkflow', () => {
    expect(() => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-unsafe-assignment
      const { Workflow } = require('./workflow.decorator');
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call
      @Workflow({ name: 'bad-workflow' })
      class BadWorkflow {
        run(): Promise<void> {
          return Promise.resolve();
        }
      }
      void BadWorkflow;
    }).toThrow('must extend DozerWorkflow');
  });

  it('throws when a method has neither @Step nor @NoStep', () => {
    expect(() => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-unsafe-assignment
      const { Workflow } = require('./workflow.decorator');
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call
      @Workflow({ name: 'unannotated-workflow' })
      class UnannotatedWorkflow extends DozerWorkflow<unknown> {
        @Step({ name: 'step-one' })
        stepOne(): Promise<void> {
          return Promise.resolve();
        }

        unannotatedHelper(): Promise<void> {
          return Promise.resolve();
        }

        async run(): Promise<void> {}
      }
      void UnannotatedWorkflow;
    }).toThrow('unannotatedHelper');
  });

  it('passes when all non-run methods have @Step or @NoStep', () => {
    expect(() => {
      // eslint-disable-next-line @typescript-eslint/no-require-imports, @typescript-eslint/no-unsafe-assignment
      const { Workflow } = require('./workflow.decorator');
      // eslint-disable-next-line @typescript-eslint/no-unsafe-call
      @Workflow({ name: 'valid-workflow' })
      class ValidWorkflow extends DozerWorkflow<unknown> {
        @Step({ name: 'step-one' })
        stepOne(): Promise<void> {
          return Promise.resolve();
        }

        @NoStep()
        helperMethod(): void {}

        async run(): Promise<void> {}
      }
      void ValidWorkflow;
    }).not.toThrow();
  });
});
