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

const EXEMPT_METHODS = new Set(['constructor', 'run']);

const validateWorkflowClass = (target: object): void => {
  if (
    !(target as Function).prototype ||
    !((target as Function).prototype instanceof DozerWorkflow)
  ) {
    throw new Error(
      `Workflow "${(target as Function).name}" must extend DozerWorkflow.`,
    );
  }

  const methodNames = Object.getOwnPropertyNames(
    (target as Function).prototype,
  ).filter((name) => !EXEMPT_METHODS.has(name));

  for (const name of methodNames) {
    const descriptor = Object.getOwnPropertyDescriptor(
      (target as Function).prototype,
      name,
    );
    if (!descriptor || typeof descriptor.value !== 'function') {
      continue;
    }

    const hasStep =
      Reflect.getMetadata(STEP_OPTIONS_METADATA, descriptor.value) !== undefined;
    const hasNoStep =
      Reflect.getMetadata(NOSTEP_METADATA, descriptor.value) === true;

    if (!hasStep && !hasNoStep) {
      throw new Error(
        `Workflow "${(target as Function).name}" method "${name}" must be decorated with @Step() or @NoStep().`,
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
