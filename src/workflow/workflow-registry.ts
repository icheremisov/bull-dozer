import { Injectable, Type } from '@nestjs/common';
import { WorkflowExecutionOptions } from '../decorators/workflow.decorator';
import { WorkflowNotRegisteredError } from '../errors/workflow-not-registered.error';
import { WorkflowJobOptions } from '../queue/workflow-queue';

export interface RunnableWorkflow {
  run(input: unknown): Promise<unknown>;
}

export interface RegisteredWorkflowOptions {
  job?: WorkflowJobOptions;
  execution?: WorkflowExecutionOptions;
}

export interface RegisteredWorkflow {
  factory: () => RunnableWorkflow;
  workflowClass: Type<unknown>;
  options: RegisteredWorkflowOptions;
}

const isRunnableWorkflow = (value: unknown): value is RunnableWorkflow => {
  return (
    typeof value === 'object' &&
    value !== null &&
    'run' in value &&
    typeof (value as RunnableWorkflow).run === 'function'
  );
};

@Injectable()
export class WorkflowRegistry {
  private readonly workflows = new Map<string, RegisteredWorkflow>();

  register(
    workflowName: string,
    workflowClass: Type<unknown>,
    factory: () => RunnableWorkflow,
    options: RegisteredWorkflowOptions = {},
  ): void {
    const existing = this.workflows.get(workflowName);
    if (existing && existing.workflowClass !== workflowClass) {
      throw new Error(
        `Workflow "${workflowName}" is already registered with another instance.`,
      );
    }

    this.workflows.set(workflowName, {
      factory,
      workflowClass,
      options,
    });
  }

  resolve(workflowName: string): RunnableWorkflow {
    const definition = this.resolveDefinition(workflowName);
    return this.instantiate(definition);
  }

  resolveDefinition(workflowName: string): RegisteredWorkflow {
    const definition = this.workflows.get(workflowName);
    if (!definition) {
      throw new WorkflowNotRegisteredError(workflowName);
    }

    const instancePrototype = (
      definition.workflowClass as {
        prototype?: unknown;
      }
    ).prototype;
    if (
      typeof instancePrototype !== 'object' ||
      instancePrototype === null ||
      typeof (instancePrototype as RunnableWorkflow).run !== 'function'
    ) {
      throw new Error(
        `Workflow provider "${workflowName}" must expose "run(input)" method.`,
      );
    }

    return definition;
  }

  instantiate(definition: RegisteredWorkflow): RunnableWorkflow {
    const instance = definition.factory();
    if (!isRunnableWorkflow(instance)) {
      throw new Error(
        `Workflow provider "${definition.workflowClass.name}" must expose "run(input)" method.`,
      );
    }

    return instance;
  }

  resolveOptionalDefinition(workflowName: string): RegisteredWorkflow | null {
    const definition = this.workflows.get(workflowName);
    if (!definition) {
      return null;
    }

    return definition;
  }
}
