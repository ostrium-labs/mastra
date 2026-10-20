import type { PublicSchema } from '@mastra/core/schema';
import { createTool } from '@mastra/core/tools';
import type { Step, Workflow } from '@mastra/core/workflows';
import { cloneStep as coreCloneStep, createStep as coreCreateStep } from '@mastra/core/workflows';
import type { Resonate } from '@resonatehq/sdk/async';
import type { ResonateEngineConfig, ResonateEngineType, ResonateWorkflowConfig } from './types';
import { detectGroup, resolveConfig } from './types';
import { ResonateWorkflow } from './workflow';

export { CodecError, decodeError, decodeValue, encodeError, encodeValue } from './codec';
export { NondeterminismError, ResonateExecutionEngine } from './execution-engine';
export { ResonateRun, serializeRequestContext } from './run';
export {
  DEFAULT_RUN_TIMEOUT_MS,
  type DurablePromiseState,
  firstPassId,
  type InvocationPayload,
  type ResonateEngineConfig,
  type ResonateEngineType,
  type ResonateWorkflowConfig,
  rootIdForRun,
  statusFromPromiseState,
} from './types';
export { ResonateWorkflow } from './workflow';

/**
 * Creates a step. A `ResonateWorkflow` passed here keeps its identity, so it runs
 * as a nested durable workflow; everything else (params, agents, tools,
 * processors) is core's `createStep`.
 */
export const createStep: typeof coreCreateStep = ((params: any, options?: any) =>
  params instanceof ResonateWorkflow ? params : coreCreateStep(params, options)) as typeof coreCreateStep;

/**
 * Binds Mastra workflows to a Resonate client.
 *
 * ```ts
 * import { Resonate } from '@resonatehq/sdk/async';
 * const resonate = new Resonate({ url: 'http://127.0.0.1:8001', group: 'mastra' });
 * const { createWorkflow, createStep } = init(resonate);
 * ```
 *
 * Each workflow is registered as a Resonate function named by its id when it is
 * added to a `Mastra` instance. Every process that registers the same workflows
 * in the same group is a worker that can recover their runs.
 */
export function init(resonate: Resonate, config?: ResonateEngineConfig) {
  const resolved = resolveConfig(config, detectGroup(resonate));
  return {
    createTool,
    createStep,
    createWorkflow<
      TWorkflowId extends string = string,
      TState = any,
      TInput = any,
      TOutput = any,
      TSteps extends Step<string, any, any, any, any, any, ResonateEngineType>[] = Step<
        string,
        any,
        any,
        any,
        any,
        any,
        ResonateEngineType
      >[],
      TParsedInput = TInput,
      TRequestContext extends Record<string, any> | unknown = unknown,
    >(
      params: ResonateWorkflowConfig<TWorkflowId, TState, TInput, TOutput, TSteps, TRequestContext> & {
        inputSchema: PublicSchema<TParsedInput, TInput>;
      },
    ) {
      return new ResonateWorkflow<
        ResonateEngineType,
        TSteps,
        TWorkflowId,
        TState,
        TInput,
        TOutput,
        TParsedInput,
        TRequestContext
      >(params as any, resonate, resolved);
    },
    cloneStep<TStepId extends string>(
      step: Step<TStepId, any, any, any, any, any, ResonateEngineType>,
      opts: { id: TStepId },
    ): Step<TStepId, any, any, any, any, any, ResonateEngineType> {
      return coreCloneStep(step as any, opts) as any;
    },
    cloneWorkflow<TWorkflowId extends string = string>(
      workflow: Workflow<ResonateEngineType, any, string, any, any, any, any>,
      opts: { id: TWorkflowId },
    ): ResonateWorkflow<ResonateEngineType, any, TWorkflowId, any, any, any, any> {
      const wf = new ResonateWorkflow<ResonateEngineType, any, TWorkflowId, any, any, any, any>(
        {
          id: opts.id,
          inputSchema: workflow.inputSchema,
          outputSchema: workflow.outputSchema,
          steps: workflow.stepDefs,
          mastra: workflow.mastra,
          options: workflow.options,
        } as any,
        resonate,
        resolved,
      );
      wf.setStepFlow(workflow.stepGraph);
      wf.commit();
      return wf;
    },
  };
}
