import { Module } from '@nestjs/common';
import { Queue } from 'bullmq';
import { DozerModule } from 'dozer';
import { AppController } from './app.controller';
import { QueueModule } from './infra/queue.module';
import { WorkflowWorkerService } from './infra/workflow-worker.service';
import { EXAMPLE_RESULT_QUEUE, EXAMPLE_WORKFLOW_QUEUE } from './infra/tokens';
import { BranchSelectorService } from './support/branch-selector.service';
import { FailureMemoryService } from './support/failure-memory.service';
import { ScenarioControlsService } from './support/scenario-controls.service';
import { WorkflowJoinService } from './support/workflow-join.service';
import { ActionNondeterministicWorkflow } from './workflows/action-nondeterministic.workflow';
import { FailurePublishWorkflow } from './workflows/failure-publish.workflow';
import { OnFailedWorkflow } from './workflows/on-failed.workflow';
import { BatchWaitWorkflow } from './workflows/batch-wait.workflow';
import { BinaryPayloadWorkflow } from './workflows/binary-payload.workflow';
import { ChildDeepWorkflow } from './workflows/child-deep.workflow';
import { ChildFailingWorkflow } from './workflows/child-failing.workflow';
import { ChildWorkflow } from './workflows/child.workflow';
import { DateSerializationWorkflow } from './workflows/date-serialization.workflow';
import { DuplicateStepNameWorkflow } from './workflows/duplicate-step-name.workflow';
import { FlakyWorkflow } from './workflows/flaky.workflow';
import { GrandchildWorkflow } from './workflows/grandchild.workflow';
import { InheritanceDispatchWorkflow } from './workflows/inheritance-dispatch.workflow';
import { InheritancePlainOverrideWorkflow } from './workflows/inheritance-plain-override.workflow';
import { InheritanceWorkflow } from './workflows/inheritance.workflow';
import { InputValidationWorkflow } from './workflows/input-validation.workflow';
import { LongRunningWorkflow } from './workflows/long-running.workflow';
import { MissingStepWorkflow } from './workflows/missing-step.workflow';
import { NestedWorkflow } from './workflows/nested.workflow';
import { NoStepWorkflow } from './workflows/no-step.workflow';
import { NonDeterministicWorkflow } from './workflows/non-deterministic.workflow';
import { ParentChildFailingWorkflow } from './workflows/parent-child-failing.workflow';
import { ParentDeepWorkflow } from './workflows/parent-deep.workflow';
import { ParentWorkflow } from './workflows/parent.workflow';
import { RecursiveWorkflow } from './workflows/recursive.workflow';
import { ReplayWorkflow } from './workflows/replay.workflow';
import { ResultQueueWorkflow } from './workflows/result-queue.workflow';
import { ResultQueueTypedWorkflow } from './workflows/result-queue-typed.workflow';
import { RepeatedStepWorkflow } from './workflows/repeated-step.workflow';
import { RunSourceNondeterministicWorkflow } from './workflows/run-source-nondeterministic.workflow';
import { SimpleWorkflow } from './workflows/simple.workflow';
import { SyncAsyncWorkflow } from './workflows/sync-async.workflow';
import { StepOutsideFlowWorkflow } from './workflows/step-outside-flow.workflow';
import { ThisStateWorkflow } from './workflows/this-state.workflow';
import { TypedInputWorkflow } from './workflows/typed-input.workflow';
import { TypedStepWorkflow } from './workflows/typed-step.workflow';
import { VersionedLogicWorkflow } from './workflows/versioned-logic.workflow';
import { SleepWorkflow } from './workflows/sleep.workflow';
import { SignalWorkflow } from './workflows/signal.workflow';
import { PollingWorkflow } from './workflows/polling.workflow';

@Module({
  imports: [
    QueueModule,
    DozerModule.forRootAsync({
      imports: [QueueModule],
      inject: [EXAMPLE_WORKFLOW_QUEUE, EXAMPLE_RESULT_QUEUE],
      useFactory: (queue: Queue, resultQueue: Queue) => ({
        queue,
        resultQueue,
      }),
    }),
    DozerModule.forFeature(
      [
        SimpleWorkflow,
        TypedInputWorkflow,
        TypedStepWorkflow,
        NestedWorkflow,
        FlakyWorkflow,
        LongRunningWorkflow,
        ReplayWorkflow,
        ResultQueueWorkflow,
        ResultQueueTypedWorkflow,
        RepeatedStepWorkflow,
        NonDeterministicWorkflow,
        NoStepWorkflow,
        RunSourceNondeterministicWorkflow,
        ThisStateWorkflow,
        RecursiveWorkflow,
        ActionNondeterministicWorkflow,
        BinaryPayloadWorkflow,
        DateSerializationWorkflow,
        ChildWorkflow,
        ParentWorkflow,
        ChildFailingWorkflow,
        ParentChildFailingWorkflow,
        GrandchildWorkflow,
        ChildDeepWorkflow,
        ParentDeepWorkflow,
        BatchWaitWorkflow,
        SyncAsyncWorkflow,
        MissingStepWorkflow,
        VersionedLogicWorkflow,
        InputValidationWorkflow,
        DuplicateStepNameWorkflow,
        InheritanceWorkflow,
        InheritanceDispatchWorkflow,
        InheritancePlainOverrideWorkflow,
        StepOutsideFlowWorkflow,
        OnFailedWorkflow,
        FailurePublishWorkflow,
        SleepWorkflow,
        SignalWorkflow,
        PollingWorkflow,
      ],
      [
        FailureMemoryService,
        BranchSelectorService,
        ScenarioControlsService,
        WorkflowJoinService,
      ],
    ),
  ],
  controllers: [AppController],
  providers: [WorkflowWorkerService],
})
export class AppModule {}
