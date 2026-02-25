export const EXAMPLE_REDIS_CONFIG = Symbol('EXAMPLE_REDIS_CONFIG');
export const EXAMPLE_REDIS_CONNECTION = Symbol('EXAMPLE_REDIS_CONNECTION');
export const EXAMPLE_WORKFLOW_QUEUE = Symbol('EXAMPLE_WORKFLOW_QUEUE');
export const EXAMPLE_RESULT_QUEUE = Symbol('EXAMPLE_RESULT_QUEUE');

const envWorkflowQueueName = process.env.EXAMPLE_WORKFLOW_QUEUE_NAME?.trim();
const envResultQueueName = process.env.EXAMPLE_RESULT_QUEUE_NAME?.trim();

export const EXAMPLE_WORKFLOW_QUEUE_NAME =
  envWorkflowQueueName && envWorkflowQueueName.length > 0
    ? envWorkflowQueueName
    : 'wf-example';
export const EXAMPLE_RESULT_QUEUE_NAME =
  envResultQueueName && envResultQueueName.length > 0
    ? envResultQueueName
    : `${EXAMPLE_WORKFLOW_QUEUE_NAME}-results`;
