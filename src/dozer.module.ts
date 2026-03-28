import {
  Abstract,
  DynamicModule,
  Global,
  Module,
  ModuleMetadata,
  Provider,
  Type,
} from '@nestjs/common';
import {
  DOZER_MODULE_OPTIONS,
  DOZER_QUEUE_DRIVER,
  DOZER_WORKFLOW_REGISTRAR,
  WORKFLOW_OPTIONS_METADATA,
} from './constants';
import { DozerClient } from './client/dozer-client';
import { DozerEngine } from './engine/dozer-engine';
import { BullMQWorkflowQueue } from './queue/bullmq-workflow-queue';
import {
  BullMQQueueLike,
  WorkflowResultQueueJobData,
  WorkflowJobOptions,
  WorkflowQueueDriver,
} from './queue/workflow-queue';
import {
  WorkflowExecutionOptions,
  WorkflowOptions,
} from './decorators/workflow.decorator';
import { WorkflowRegistry } from './workflow/workflow-registry';

export interface DozerDefaultsOptions {
  job?: WorkflowJobOptions;
  execution?: WorkflowExecutionOptions;
}

export interface DozerModuleOptions {
  driver?: WorkflowQueueDriver;
  queue?: BullMQQueueLike<unknown>;
  resultQueue?: BullMQQueueLike<WorkflowResultQueueJobData<unknown>>;
  defaults?: DozerDefaultsOptions;
  onWorkflowFailed?: (
    jobId: string,
    workflowName: string,
    error: Error,
  ) => Promise<void> | void;
}

export interface DozerModuleAsyncOptions extends Pick<
  ModuleMetadata,
  'imports'
> {
  inject?: Array<string | symbol | Type<unknown> | Abstract<unknown>>;
  useFactory: (
    ...args: unknown[]
  ) => DozerModuleOptions | Promise<DozerModuleOptions>;
}

const createQueueDriverProvider = (): Provider => ({
  provide: DOZER_QUEUE_DRIVER,
  useFactory: (options: DozerModuleOptions): WorkflowQueueDriver => {
    if (options.driver) {
      return options.driver;
    }

    if (options.queue) {
      return new BullMQWorkflowQueue(options.queue);
    }

    throw new Error(
      'DozerModule requires either "driver" or BullMQ "queue" in forRoot options.',
    );
  },
  inject: [DOZER_MODULE_OPTIONS],
});

const createWorkflowRegistrarProvider = (
  workflows: Type<unknown>[],
  workflowFactoryTokens: Array<string | symbol>,
): Provider => ({
  provide: DOZER_WORKFLOW_REGISTRAR,
  useFactory: (
    registry: WorkflowRegistry,
    ...workflowFactories: Array<
      () => { run: (input: unknown) => Promise<unknown> }
    >
  ): true => {
    for (let i = 0; i < workflows.length; i += 1) {
      const workflowClass = workflows[i];
      const factory = workflowFactories[i];
      const options = Reflect.getMetadata(
        WORKFLOW_OPTIONS_METADATA,
        workflowClass,
      ) as WorkflowOptions | undefined;

      if (!options?.name) {
        throw new Error(
          `Workflow "${workflowClass.name}" must be decorated with @Workflow({ name }).`,
        );
      }

      const prototype = workflowClass.prototype as
        | { run?: (input: unknown) => Promise<unknown> }
        | undefined;
      if (!prototype || typeof prototype.run !== 'function') {
        throw new Error(
          `Workflow "${workflowClass.name}" must implement run(input): Promise<unknown>.`,
        );
      }

      registry.register(options.name, workflowClass, factory, {
        job: options.job,
        execution: options.execution,
        resultQueue: options.resultQueue,
      });
    }

    return true;
  },
  inject: [WorkflowRegistry, ...workflowFactoryTokens],
});

const getWorkflowFactoryToken = (workflowClass: Type<unknown>): string => {
  return `DOZER_WORKFLOW_FACTORY:${workflowClass.name}`;
};

const createWorkflowFactoryProviders = (
  workflows: Type<unknown>[],
): Provider[] => {
  return workflows.map((workflowClass) => {
    const constructorParamTypes =
      (Reflect.getMetadata('design:paramtypes', workflowClass) as Array<
        string | symbol | Type<unknown> | Abstract<unknown>
      >) ?? [];
    const explicitInjectTokens =
      (Reflect.getMetadata('self:paramtypes', workflowClass) as Array<{
        index: number;
        param: string | symbol | Type<unknown> | Abstract<unknown>;
      }>) ?? [];

    const injectTokens = [...constructorParamTypes];
    for (const explicitToken of explicitInjectTokens) {
      injectTokens[explicitToken.index] = explicitToken.param;
    }

    return {
      provide: getWorkflowFactoryToken(workflowClass),
      useFactory: (...deps: unknown[]): (() => unknown) => {
        return () =>
          new (workflowClass as new (...args: unknown[]) => unknown)(...deps);
      },
      inject: injectTokens,
    };
  });
};

@Global()
@Module({})
export class DozerModule {
  static forRoot(options: DozerModuleOptions): DynamicModule {
    return {
      module: DozerModule,
      providers: [
        {
          provide: DOZER_MODULE_OPTIONS,
          useValue: options,
        },
        createQueueDriverProvider(),
        WorkflowRegistry,
        DozerClient,
        DozerEngine,
      ],
      exports: [DozerClient, DozerEngine, WorkflowRegistry],
    };
  }

  static forRootAsync(options: DozerModuleAsyncOptions): DynamicModule {
    return {
      module: DozerModule,
      imports: [...(options.imports ?? [])],
      providers: [
        {
          provide: DOZER_MODULE_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject ?? [],
        },
        createQueueDriverProvider(),
        WorkflowRegistry,
        DozerClient,
        DozerEngine,
      ],
      exports: [DozerClient, DozerEngine, WorkflowRegistry],
    };
  }

  static forClient(options: DozerModuleOptions): DynamicModule {
    return {
      module: DozerModule,
      providers: [
        {
          provide: DOZER_MODULE_OPTIONS,
          useValue: options,
        },
        createQueueDriverProvider(),
        DozerClient,
      ],
      exports: [DozerClient],
    };
  }

  static forClientAsync(options: DozerModuleAsyncOptions): DynamicModule {
    return {
      module: DozerModule,
      imports: [...(options.imports ?? [])],
      providers: [
        {
          provide: DOZER_MODULE_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject ?? [],
        },
        createQueueDriverProvider(),
        DozerClient,
      ],
      exports: [DozerClient],
    };
  }

  static forFeature(
    workflows: Type<unknown>[],
    providers: Provider[] = [],
  ): DynamicModule {
    const workflowFactoryProviders = createWorkflowFactoryProviders(workflows);
    const workflowFactoryTokens = workflows.map((workflowClass) =>
      getWorkflowFactoryToken(workflowClass),
    );

    return {
      module: DozerModule,
      providers: [
        ...providers,
        ...workflows,
        ...workflowFactoryProviders,
        createWorkflowRegistrarProvider(workflows, workflowFactoryTokens),
      ],
      exports: [...providers, ...workflows],
    };
  }
}
