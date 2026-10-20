import { RequestContext } from '@mastra/core/di';
import { getErrorFromUnknown, MastraError } from '@mastra/core/error';
import type { Mastra } from '@mastra/core/mastra';
import { EntityType, SpanType } from '@mastra/core/observability';
import type { WorkflowRuns } from '@mastra/core/storage';
import type {
  Run,
  Step,
  StepFlowEntry,
  StepResult,
  WorkflowConfig,
  WorkflowResult,
  WorkflowRunState,
  WorkflowStreamEvent,
} from '@mastra/core/workflows';
import { getEntryWorkflow, isSingleStepEntry, Workflow } from '@mastra/core/workflows';
import type { Context, Resonate } from '@resonatehq/sdk/async';
import { decodeValue, encodeError, encodeValue } from './codec';
import { currentContext, durable, ResonateExecutionEngine, withContext } from './execution-engine';
import { resolveResumeSteps, validateWithSchema } from './resume';
import { ResonateRun } from './run';
import type {
  InvocationPayload,
  ResolvedEngineConfig,
  ResonateEngineType,
  ResonateWorkflowConfig,
  ResumeRequest,
} from './types';

/** Workflows registered on each Resonate client, by workflow id. */
const registered = new WeakMap<Resonate, Map<string, ResonateWorkflow<any, any, any, any, any, any, any, any>>>();

function getNestedResonateWorkflow(entry: StepFlowEntry): ResonateWorkflow | null {
  let nested: unknown = null;
  if (entry.type === 'loop' || entry.type === 'foreach') {
    nested = getEntryWorkflow(entry.step as any);
  } else if (isSingleStepEntry(entry)) {
    nested = getEntryWorkflow(entry);
  }
  return nested instanceof ResonateWorkflow ? nested : null;
}

export class ResonateWorkflow<
  TEngineType = ResonateEngineType,
  TSteps extends Step<string, any, any, any, any, any, TEngineType, any>[] = Step<
    string,
    unknown,
    unknown,
    unknown,
    unknown,
    unknown,
    TEngineType,
    unknown
  >[],
  TWorkflowId extends string = string,
  TState = unknown,
  TInput = unknown,
  TOutput = unknown,
  TPrevSchema = TInput,
  TRequestContext extends Record<string, any> | unknown = unknown,
> extends Workflow<TEngineType, TSteps, TWorkflowId, TState, TInput, TOutput, TPrevSchema, TRequestContext> {
  #mastra: Mastra | undefined;
  readonly resonate: Resonate;
  readonly #config: ResolvedEngineConfig;

  constructor(
    params: ResonateWorkflowConfig<
      TWorkflowId,
      TState,
      TInput,
      TOutput,
      TSteps & Step<string, any, any, any, any, any, ResonateEngineType, any>[],
      TRequestContext
    >,
    resonate: Resonate,
    config: ResolvedEngineConfig,
  ) {
    super(params as WorkflowConfig<TWorkflowId, TState, TInput, TOutput, TSteps, TRequestContext>);
    this.engineType = 'resonate';
    this.#mastra = params.mastra;
    this.resonate = resonate;
    this.#config = config;
  }

  get config(): ResolvedEngineConfig {
    return this.#config;
  }

  __registerMastra(mastra: Mastra) {
    super.__registerMastra(mastra);
    this.#mastra = mastra;
    this.executionEngine.__registerMastra(mastra);
    this.register();
    this.#forEachNested(nested => nested.__registerMastra(mastra));
  }

  /**
   * Registers this workflow as a Resonate function named by its id, so that this
   * process can run it and recover its runs after a crash. Called when the
   * workflow is added to a Mastra instance.
   */
  register(): void {
    let byId = registered.get(this.resonate);
    if (!byId) {
      byId = new Map();
      registered.set(this.resonate, byId);
    }
    const isNew = !byId.has(this.id);
    byId.set(this.id, this);
    if (!isNew) return;

    const workflowId = this.id;
    const byIdRef = byId;
    this.resonate.register(workflowId, async function mastraWorkflow(ctx: Context, payload: unknown) {
      const workflow = byIdRef.get(workflowId);
      if (!workflow) throw new Error(`@mastra/resonate: workflow "${workflowId}" is not registered`);
      return workflow.runRoot(ctx, decodeValue<InvocationPayload>(payload));
    });
  }

  #forEachNested(fn: (nested: ResonateWorkflow) => void) {
    const visit = (entry: StepFlowEntry) => {
      const nested = getNestedResonateWorkflow(entry);
      if (nested) {
        fn(nested);
      } else if (entry.type === 'parallel' || entry.type === 'conditional') {
        for (const sub of entry.steps) visit(sub);
      }
    };
    for (const entry of this.executionGraph.steps ?? []) visit(entry);
  }

  async listWorkflowRuns(args?: {
    fromDate?: Date;
    toDate?: Date;
    perPage?: number | false;
    page?: number;
    resourceId?: string;
  }) {
    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    if (!workflowsStore) return { runs: [], total: 0 };
    return workflowsStore.listWorkflowRuns({
      workflowName: this.id,
      ...(args ?? {}),
    }) as unknown as WorkflowRuns;
  }

  async createRun(options?: {
    runId?: string;
    resourceId?: string;
    disableScorers?: boolean;
  }): Promise<Run<TEngineType, TSteps, TState, TInput, TOutput, TRequestContext>> {
    // The same checks as core's Workflow.createRun.
    if (this.stepFlow.length === 0) {
      throw new Error(
        'Execution flow of workflow is not defined. Add steps to the workflow via .then(), .branch(), etc.',
      );
    }
    if (!this.executionGraph.steps) {
      throw new Error('Uncommitted step flow changes detected. Call .commit() to register the steps.');
    }
    const runId = options?.runId || globalThis.crypto.randomUUID();
    const existing = this.runs.get(runId);
    const run = (existing ??
      new ResonateRun<TEngineType, TSteps, TState, TInput, TOutput, TRequestContext>(
        {
          workflowId: this.id,
          runId,
          resourceId: options?.resourceId,
          stateSchema: this.stateSchema,
          inputSchema: this.inputSchema,
          requestContextSchema: this.requestContextSchema,
          disableScorers: options?.disableScorers,
          tracingPolicy: this.options.tracingPolicy,
          executionEngine: this.executionEngine,
          executionGraph: this.executionGraph,
          serializedStepGraph: this.serializedStepGraph,
          mastra: this.#mastra,
          retryConfig: this.retryConfig,
          cleanup: () => this.runs.delete(runId),
          workflowSteps: this.steps,
          workflowEngineType: this.engineType,
          validateInputs: this.options.validateInputs,
          pubsub: this.#mastra?.pubsub,
        },
        this.resonate,
        this.#config,
      )) as Run<TEngineType, TSteps, TState, TInput, TOutput, TRequestContext>;
    this.runs.set(runId, run);

    const shouldPersistSnapshot = this.options.shouldPersistSnapshot({
      workflowStatus: run.workflowRunStatus,
      stepResults: {},
    });
    const stored = await this.getWorkflowRunById(runId, { withNestedWorkflows: false });
    if (!(stored && !stored.isFromInMemory) && shouldPersistSnapshot) {
      const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
      await workflowsStore?.persistWorkflowSnapshot({
        workflowName: this.id,
        runId,
        resourceId: options?.resourceId,
        snapshot: {
          runId,
          status: 'pending',
          value: {},
          context: {},
          activePaths: [],
          activeStepsPath: {},
          waitingPaths: {},
          serializedStepGraph: this.serializedStepGraph,
          suspendedPaths: {},
          resumeLabels: {},
          result: undefined,
          error: undefined,
          timestamp: Date.now(),
        },
      });
    }
    return run;
  }

  /**
   * The body of the Resonate function. A `root` call dispatches the pass as a
   * durable remote call (`<root>:0`, a global promise a caller can wait on, and
   * its own task, recoverable on any worker of the group) and waits for it. A
   * `pass` call executes the graph.
   */
  async runRoot(ctx: Context, payload: InvocationPayload): Promise<unknown> {
    if (payload.role === 'pass') {
      return encodeValue(await this.runInvocation(ctx, payload));
    }
    const tags = { 'mastra:workflowId': this.id, 'mastra:runId': payload.runId };
    let pass: InvocationPayload = { ...payload, role: 'pass' };
    for (let k = 0; ; k++) {
      const encoded = await ctx.rpc(
        this.id,
        encodeValue(pass),
        ctx.options({
          target: this.#config.group,
          timeout: this.#config.timeout,
          tags: { ...tags, 'mastra:op': `pass:${k}` },
        }),
      );
      if (decodeValue<{ status?: string }>(encoded)?.status !== 'suspended') return encoded;

      // Suspended: wait, durably, for whoever resumes the run to settle this
      // promise with a ResumeRequest. It is a plain Resonate promise, so a
      // client outside Mastra may settle it too.
      const request = await ctx.promise<unknown>({
        timeout: this.#config.timeout,
        tags: { ...tags, 'mastra:op': `resume:${k}` },
      });
      pass = {
        kind: 'resume',
        role: 'pass',
        workflowId: this.id,
        runId: payload.runId,
        resourceId: payload.resourceId,
        requestContext: {},
        outputOptions: payload.outputOptions,
        format: payload.format,
        resumeRequest: decodeValue<ResumeRequest>(request),
      };
    }
  }

  /**
   * Executes one pass of this workflow under `ctx`: the Mastra graph on a
   * Resonate-backed engine, then a memoised finalize step (lifecycle callbacks,
   * the workflow span, the final snapshot and the finish event).
   */
  async runInvocation(
    ctx: Context,
    payload: InvocationPayload,
  ): Promise<WorkflowResult<any, any, any, any> | Record<string, unknown>> {
    return withContext(ctx, () => this.#runInvocation(payload));
  }

  async #runInvocation(payload: InvocationPayload) {
    const mastra = this.#mastra;
    const { runId, resourceId, outputOptions, format } = payload;
    let { inputData, perStep } = payload;
    let initialState = payload.initialState as any;
    let requestContextEntries = payload.requestContext ?? {};
    let actor = payload.actor;
    let resume: any;

    if (payload.kind === 'resume') {
      const prepared = await durable(`workflow.${this.id}.resume.prepare`, this.#config.timeout, () =>
        this.#prepareResume(payload),
      );
      if ('rejected' in prepared) {
        // The run stays suspended; the caller gets the reason.
        return { ...prepared.suspended, resumeRejected: prepared.rejected };
      }
      resume = prepared.resume;
      initialState = prepared.state;
      inputData = prepared.input;
      requestContextEntries = prepared.requestContext;
      actor = payload.resumeRequest?.actor ?? actor;
      perStep = payload.resumeRequest?.perStep ?? perStep;
    }
    const requestContext = new RequestContext<any>(Object.entries(requestContextEntries) as [string, any][]);
    const pubsub = mastra?.pubsub;
    if (!pubsub) {
      throw new Error(`@mastra/resonate: workflow "${this.id}" must be registered with a Mastra instance`);
    }
    const tracingPolicy = this.options.tracingPolicy;
    const timeout = this.#config.timeout;

    // The run's root span is created once and rebuilt on every replay.
    const workflowSpanData = mastra?.observability?.getSelectedInstance({ requestContext })
      ? await durable(`workflow.${this.id}.span.start`, timeout, async () => {
          const observability = mastra?.observability?.getSelectedInstance({ requestContext });
          const span = observability?.startSpan({
            type: SpanType.WORKFLOW_RUN,
            name: `workflow run: '${this.id}'`,
            entityType: EntityType.WORKFLOW_RUN,
            entityId: this.id,
            entityName: this.id,
            input: inputData,
            metadata: { resourceId, runId },
            tracingPolicy,
            tracingOptions: payload.tracingOptions as any,
            requestContext,
          });
          return span?.exportSpan();
        })
      : undefined;

    const engine = new ResonateExecutionEngine(mastra!, currentContext().originId, this.options, this.#config);

    let result: WorkflowResult<any, any, any, any>;
    try {
      result = await engine.execute<any, any, WorkflowResult<any, any, any, any>>({
        workflowId: this.id,
        runId,
        resourceId,
        graph: this.executionGraph,
        serializedStepGraph: this.serializedStepGraph,
        input: inputData,
        initialState,
        pubsub,
        retryConfig: this.retryConfig,
        requestContext,
        actor: actor as any,
        resume,
        perStep,
        format,
        abortController: new AbortController(),
        workflowSpan: undefined,
        tracingIds: workflowSpanData
          ? { traceId: workflowSpanData.traceId, workflowSpanId: workflowSpanData.id }
          : undefined,
        outputOptions,
        outputWriter: async (chunk: WorkflowStreamEvent) => {
          try {
            await pubsub.publish(`workflow.events.v2.${runId}`, {
              type: 'watch',
              runId,
              data: chunk,
            });
          } catch (err) {
            this.logger.debug?.('Failed to publish watch event:', err);
          }
        },
      });
    } catch (executionError) {
      result = {
        status: 'failed',
        steps: {},
        state: initialState ?? {},
        error: executionError instanceof Error ? executionError : new Error(String(executionError)),
      } as WorkflowResult<any, any, any, any>;
    }

    await durable(`workflow.${this.id}.finalize`, timeout, async () => {
      if (result.status !== 'paused') {
        await engine.invokeLifecycleCallbacksInternal({
          status: result.status,
          result: 'result' in result ? result.result : undefined,
          error: 'error' in result ? result.error : undefined,
          steps: result.steps,
          tripwire: 'tripwire' in result ? result.tripwire : undefined,
          runId,
          workflowId: this.id,
          resourceId,
          input: inputData,
          requestContext,
          state: result.state ?? initialState ?? {},
        });
      }

      if (workflowSpanData) {
        const observability = mastra?.observability?.getSelectedInstance({ requestContext });
        if (observability) {
          const workflowSpan = observability.rebuildSpan(workflowSpanData);
          if (result.status === 'failed') {
            workflowSpan.error({
              error: getErrorFromUnknown(result.error, {
                fallbackMessage: 'Workflow execution failed',
              }),
              attributes: { status: 'failed' },
            });
          } else {
            workflowSpan.end({
              output: result.status === 'success' ? result.result : undefined,
              attributes: { status: result.status },
            });
          }
        }
      }

      // The final snapshot. Events match the default engine's: a stream's
      // workflow-finish comes from core's Run when the result arrives.
      if (
        this.options.shouldPersistSnapshot({
          workflowStatus: result.status,
          stepResults: result.steps,
        })
      ) {
        const workflowsStore = await mastra?.getStorage()?.getStore('workflows');
        if (workflowsStore) {
          let existing:
            | {
                suspendedPaths?: Record<string, number[]>;
                resumeLabels?: Record<string, any>;
                requestContext?: Record<string, any>;
              }
            | undefined;
          if (result.status === 'suspended') {
            existing = (await workflowsStore.loadWorkflowSnapshot({ workflowName: this.id, runId })) ?? undefined;
          }
          const finalSnapshot: WorkflowRunState = {
            runId,
            status: result.status,
            value: result.state ?? initialState ?? {},
            context: result.steps as unknown as WorkflowRunState['context'],
            activePaths: [],
            stepExecutionPath: (result as { stepExecutionPath?: string[] }).stepExecutionPath,
            activeStepsPath: {},
            serializedStepGraph: this.serializedStepGraph,
            suspendedPaths: existing?.suspendedPaths ?? {},
            waitingPaths: {},
            resumeLabels: existing?.resumeLabels ?? (result as any).resumeLabels ?? {},
            result: result.status === 'success' ? (result.result as WorkflowRunState['result']) : undefined,
            error: result.status === 'failed' ? (result.error as any) : undefined,
            // A suspended run keeps the request context its steps left (the default
            // engine persisted it when the step suspended); resume continues from it.
            requestContext: existing?.requestContext ?? requestContext.toJSON(),
            tracingContext: workflowSpanData
              ? { traceId: workflowSpanData.traceId, spanId: workflowSpanData.id }
              : undefined,
            timestamp: Date.now(),
          };
          await workflowsStore.persistWorkflowSnapshot({
            workflowName: this.id,
            runId,
            resourceId,
            snapshot: this.options.pruneSnapshot
              ? this.options.pruneSnapshot({
                  snapshot: finalSnapshot,
                  workflowStatus: result.status,
                })
              : finalSnapshot,
          });
        }
      }
    });

    const observability = mastra?.observability?.getSelectedInstance({ requestContext });
    if (observability) {
      try {
        await observability.flush();
      } catch (flushError) {
        this.logger.debug?.('Failed to flush observability:', flushError);
      }
    }

    return payload.nested?.compact ? compact(result) : result;
  }

  /**
   * Turns a resume into the engine's resume parameters, from the run's
   * snapshot: the step path (label, step or the only suspended step), the
   * validated resume data, the request context (snapshot values, overridden by
   * the request's), and the state and step results to resume from. Runs once per
   * pass, memoised. A request that cannot resume the run is rejected without
   * executing anything.
   */
  async #prepareResume(payload: InvocationPayload): Promise<
    | {
        resume: {
          steps: string[];
          stepResults: Record<string, any>;
          resumePayload: unknown;
          resumePath: number[];
          stepExecutionPath?: string[];
          forEachIndex?: number;
          label?: string;
        };
        state: unknown;
        input: unknown;
        requestContext: Record<string, unknown>;
      }
    | { rejected: unknown; suspended: Record<string, unknown> }
  > {
    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    if (!workflowsStore) throw new Error(`@mastra/resonate: resuming workflow "${this.id}" needs storage`);
    const snapshot = await workflowsStore.loadWorkflowSnapshot({
      workflowName: this.id,
      runId: payload.runId,
    });
    if (!snapshot) throw new Error(`No snapshot found for this workflow run: ${this.id} ${payload.runId}`);

    let steps: string[];
    let resumePayload: unknown;
    let requestContext: Record<string, unknown> = { ...((snapshot as any).requestContext ?? {}) };
    const request = payload.resumeRequest;
    if (payload.resume) {
      steps = payload.resume.steps;
      resumePayload = payload.resume.resumePayload;
      requestContext = { ...requestContext, ...payload.requestContext };
    } else {
      try {
        steps = resolveResumeSteps(snapshot, { step: request?.step, label: request?.label });
        // A target that names a nested workflow alone resolves inside its snapshot now,
        // so an ambiguous target is rejected here and the run stays suspended.
        const nested = this.steps[steps[0] ?? ''];
        if (steps.length === 1 && nested instanceof ResonateWorkflow) {
          const childRunId =
            (snapshot.context as Record<string, any>)?.[steps[0]!]?.suspendPayload?.__workflow_meta?.runId ??
            payload.runId;
          const child = await workflowsStore.loadWorkflowSnapshot({
            workflowName: nested.id,
            runId: childRunId,
          });
          if (child) steps = [...steps, ...resolveResumeSteps(child, {})];
        }
        const current = (snapshot.context as Record<string, any>)?.[steps[0] ?? '']?.suspendedAt;
        if (request?.suspendedAt !== undefined && request.suspendedAt !== current) {
          throw new MastraError({
            id: 'RESONATE_ENGINE_ALREADY_RESUMED',
            domain: 'MASTRA_WORKFLOW',
            category: 'USER',
            text: `The suspension of step "${steps[0]}" this request answers was already resumed`,
            details: { workflowId: this.id, runId: payload.runId },
          } as any);
        }
        const suspendedStep = this.steps[steps[0] ?? ''];
        resumePayload =
          this.options.validateInputs && suspendedStep?.resumeSchema
            ? await validateWithSchema(suspendedStep.resumeSchema, request?.resumeData, 'resume data')
            : request?.resumeData;
      } catch (e) {
        return {
          rejected: encodeError(e),
          suspended: {
            status: snapshot.status,
            steps: snapshot.context,
            state: snapshot.value,
            resumeLabels: snapshot.resumeLabels,
          },
        };
      }
      requestContext = { ...requestContext, ...(request?.requestContext ?? {}) };
    }

    if (
      this.options.shouldPersistSnapshot({
        workflowStatus: 'running',
        stepResults: snapshot.context as Record<string, any>,
      })
    )
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: this.id,
        runId: payload.runId,
        resourceId: payload.resourceId,
        snapshot: {
          ...snapshot,
          status: 'running',
          result: undefined,
          error: undefined,
          timestamp: Date.now(),
        } as any,
      });

    return {
      resume: {
        steps,
        stepResults: snapshot.context as Record<string, any>,
        resumePayload,
        stepExecutionPath: (snapshot as { stepExecutionPath?: string[] }).stepExecutionPath,
        resumePath: (payload.resume?.resumePath ?? snapshot.suspendedPaths?.[steps[0] ?? '']) as number[],
        ...(request?.forEachIndex !== undefined ? { forEachIndex: request.forEachIndex } : {}),
        ...(request?.label ? { label: request.label } : {}),
      },
      state: snapshot.value,
      input: (snapshot.context as Record<string, any>)?.input,
      requestContext,
    };
  }
}

/** What a parent workflow needs from a nested run's result (as `@mastra/inngest` returns it). */
function compact(result: WorkflowResult<any, any, any, any>): Record<string, unknown> {
  switch (result.status) {
    case 'success':
      return { status: result.status, result: result.result, state: result.state };
    case 'failed':
      return { status: result.status, error: result.error, state: result.state };
    case 'tripwire':
      return { status: result.status, tripwire: (result as any).tripwire, state: result.state };
    case 'suspended':
      return {
        status: result.status,
        steps: result.steps as Record<string, StepResult<any, any, any, any>>,
        state: result.state,
        resumeLabels: (result as any).resumeLabels,
      };
    default:
      return { status: result.status, state: result.state };
  }
}
