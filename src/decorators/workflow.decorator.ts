import { Injectable, Type } from '@nestjs/common';
import 'reflect-metadata';
import {
  NOSTEP_METADATA,
  STEP_OPTIONS_METADATA,
  WORKFLOW_OPTIONS_METADATA,
} from '../constants';
import type { RetryOptions } from './step.decorator';
import type { WorkflowJobOptions } from '../queue/workflow-queue';
import { DozerWorkflow } from '../workflow/dozer-workflow';

export interface WorkflowResultQueueOptions {
  jobName?: string;
  job?: WorkflowJobOptions;
  publishOnFailure?: boolean;
}

export interface WorkflowExecutionOptions {
  stepRetry?: RetryOptions;
  workflowRetry?: RetryOptions;
  autoDeterminismProbe?: boolean;
  determinismProbeMaxDurationMs?: number;
}

export interface WorkflowOptions {
  name: string;
  job?: WorkflowJobOptions;
  execution?: WorkflowExecutionOptions;
  resultQueue?: WorkflowResultQueueOptions;
}

type WorkflowConstructorLike = {
  prototype: Record<string, unknown>;
  name: string;
};

const EXEMPT_METHODS = new Set(['constructor', 'run']);

const validateWorkflowClass = (target: object): void => {
  const cls = target as WorkflowConstructorLike;
  if (!cls.prototype || !(cls.prototype instanceof DozerWorkflow)) {
    throw new Error(`Workflow "${cls.name}" must extend DozerWorkflow.`);
  }

  const methodNames = Object.getOwnPropertyNames(cls.prototype).filter(
    (name) => !EXEMPT_METHODS.has(name),
  );

  for (const name of methodNames) {
    const descriptor = Object.getOwnPropertyDescriptor(cls.prototype, name);
    if (!descriptor || typeof descriptor.value !== 'function') {
      continue;
    }

    const hasStep =
      Reflect.getMetadata(STEP_OPTIONS_METADATA, descriptor.value) !==
      undefined;
    const hasNoStep =
      Reflect.getMetadata(NOSTEP_METADATA, descriptor.value) === true;

    if (!hasStep && !hasNoStep) {
      throw new Error(
        `Workflow "${cls.name}" method "${name}" must be decorated with @Step() or @NoStep().`,
      );
    }
  }
};

export function Workflow(options: WorkflowOptions): ClassDecorator {
  return (target: object) => {
    validateWorkflowClass(target);
    Reflect.defineMetadata(WORKFLOW_OPTIONS_METADATA, options, target);
    Injectable()(target as Type<unknown>);
  };
}
