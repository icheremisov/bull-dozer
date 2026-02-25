import { Injectable, Type } from '@nestjs/common';
import 'reflect-metadata';
import { WORKFLOW_OPTIONS_METADATA } from '../constants';
import type { RetryOptions } from './step.decorator';
import type { WorkflowJobOptions } from '../queue/workflow-queue';

export interface WorkflowResultQueueOptions {
  jobName?: string;
  job?: WorkflowJobOptions;
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

export function Workflow(options: WorkflowOptions): ClassDecorator {
  return (target: object) => {
    Reflect.defineMetadata(WORKFLOW_OPTIONS_METADATA, options, target);
    Injectable()(target as Type<unknown>);
  };
}
