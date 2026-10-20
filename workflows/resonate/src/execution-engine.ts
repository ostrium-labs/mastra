import { AsyncLocalStorage } from 'node:async_hooks';
import type { RequestContext } from '@mastra/core/di';
import type { SerializedError } from '@mastra/core/error';
import { getErrorFromUnknown, MastraNonRetryableError } from '@mastra/core/error';
import type { Mastra } from '@mastra/core/mastra';
import type { ExecutionContext, ExecutionEngineOptions, Step, StepFailure, StepResult } from '@mastra/core/workflows';
import { DefaultExecutionEngine } from '@mastra/core/workflows';
import type { Context } from '@resonatehq/sdk/async';
import { decodeError, decodeValue, encodeError, encodeValue } from './codec';
import { resolveResumeSteps } from './resume';
import type { InvocationPayload, ResolvedEngineConfig } from './types';
import { ResonateWorkflow } from './workflow';

type ExecuteStepParams = Parameters<DefaultExecutionEngine['executeStep']>[0];
type ExecuteAgentParams = Parameters<DefaultExecutionEngine['executeAgent']>[0];
type ExecuteToolParams = Parameters<DefaultExecutionEngine['executeTool']>[0];
type ExecuteClassifierParams = Parameters<DefaultExecutionEngine['executeClassifier']>[0];
type ExecuteMappingParams = Parameters<DefaultExecutionEngine['executeMapping']>[0];
type StepExecutionResult = Awaited<ReturnType<DefaultExecutionEngine['executeStep']>>;
type ExecuteWorkflowStepParams = Parameters<DefaultExecutionEngine['executeWorkflowStep']>[0];

/**
 * The Resonate context durable operations are created under. Resonate names a
 * child promise by its position in its parent (`root:3.1`), so concurrent
 * branches must not share a context: each step execution gets its own child
 * context, and the operations inside it run in program order.
 */
const scope = new AsyncLocalStorage<{ ctx: Context }>();
const retryCountStorage = new AsyncLocalStorage<number>();

/** Raised when a replay asks for a different operation than the one recorded at that position. */
export class NondeterminismError extends MastraNonRetryableError {
  constructor(expected: string, recorded: string | undefined) {
    super({
      id: 'RESONATE_ENGINE_NONDETERMINISM',
      domain: 'MASTRA_WORKFLOW',
      category: 'SYSTEM',
      text:
        `Replay asked for durable operation "${expected}" where "${recorded ?? '<none>'}" was recorded. ` +
        'The workflow graph or the order of its durable operations changed while a run was in flight.',
    } as any);
    this.name = 'NondeterminismError';
  }
}

type Envelope = { op: string; ok: true; v: unknown } | { op: string; ok: false; e: unknown };

/** A promise that never settles: what a continuation gets once its pass has ended. */
function parked<T>(): Promise<T> {
  return new Promise<T>(() => {});
}

/**
 * Resonate ends a pass as soon as an operation waits on a remote promise (a
 * timer, an external resolution). A concurrent branch that reaches its next
 * durable operation afterwards gets a panic from the SDK; that branch belongs to
 * a pass that is already over, so it is parked instead of failing the step.
 */
function isPassOver(error: unknown): boolean {
  const e = error as { code?: unknown; message?: unknown } | undefined;
  return e?.code === '98' && typeof e.message === 'string' && e.message.includes('after the pass ended');
}

/** Runs `fn` as a durable child of the current context, memoised under `label`. */
export async function durable<T>(
  label: string,
  timeout: number,
  fn: (ctx: Context) => Promise<T>,
  ctx: Context = currentContext(),
): Promise<T> {
  let pending: PromiseLike<Envelope>;
  try {
    pending = ctx.run(
      async function mastraOperation(child: Context): Promise<Envelope> {
        return scope.run({ ctx: child }, async () => {
          try {
            return { op: label, ok: true, v: encodeValue(await fn(child)) };
          } catch (e) {
            return { op: label, ok: false, e: encodeError(e) };
          }
        });
      },
      ctx.options({ timeout, tags: { 'mastra:op': label } }),
    );
  } catch (e) {
    if (isPassOver(e)) return parked();
    throw e;
  }
  const envelope = await pending;
  if (!envelope || envelope.op !== label) {
    throw new NondeterminismError(label, envelope?.op);
  }
  if (!envelope.ok) throw decodeError(envelope.e);
  return decodeValue<T>(envelope.v);
}

export function currentContext(): Context {
  const store = scope.getStore();
  if (!store) {
    throw new Error('@mastra/resonate: no Resonate context; the engine must run inside a registered workflow');
  }
  return store.ctx;
}

/** Runs `fn` with `ctx` as the context for durable operations. */
export function withContext<T>(ctx: Context, fn: () => Promise<T>): Promise<T> {
  return scope.run({ ctx }, fn);
}

function isNonRetryableStepFailure(error: unknown): boolean {
  if (error instanceof MastraNonRetryableError) return true;
  if (error instanceof Error && error.cause !== undefined && isNonRetryableStepFailure(error.cause)) return true;
  if (error && typeof error === 'object') {
    const record = error as { nonRetryable?: true; name?: string; isNonRetryable?: boolean };
    if (record.nonRetryable || record.isNonRetryable || record.name === 'MastraNonRetryableError') return true;
  }
  return false;
}

function entryLabel(kind: string, id: string, executionContext: ExecutionContext): string {
  const path = executionContext.executionPath.join('.');
  const index = executionContext.foreachIndex === undefined ? '' : `#${executionContext.foreachIndex}`;
  return `${kind}:${id}@${path}${index}`;
}

export class ResonateExecutionEngine extends DefaultExecutionEngine {
  readonly #config: ResolvedEngineConfig;
  readonly #rootId: string;

  constructor(mastra: Mastra, rootId: string, options: ExecutionEngineOptions, config: ResolvedEngineConfig) {
    super({ mastra, options });
    this.#config = config;
    this.#rootId = rootId;
  }

  override getOrGenerateRetryCount(_stepId: string): number {
    return retryCountStorage.getStore() ?? 0;
  }

  protected override formatResultError(
    error: Error | string | undefined,
    lastOutput: StepResult<any, any, any, any>,
  ): SerializedError {
    const outputError = (lastOutput as StepFailure<any, any, any, any>)?.error;
    const errorInstance = getErrorFromUnknown(error || outputError, {
      serializeStack: true,
      fallbackMessage: 'Unknown workflow error',
    });
    return errorInstance.toJSON();
  }

  override isNestedWorkflowStep(step: Step<any, any, any>): boolean {
    return step instanceof ResonateWorkflow;
  }

  /** Replays don't re-run memoised operations, so request-context changes must travel with their results. */
  override requiresDurableContextSerialization(): boolean {
    return true;
  }

  override getEngineContext(): Record<string, any> {
    return { ctx: currentContext(), rootId: this.#rootId };
  }

  override async wrapDurableOperation<T>(operationId: string, operationFn: () => Promise<T>): Promise<T> {
    return durable(operationId, this.#config.timeout, () => operationFn());
  }

  /**
   * Each attempt is its own durable operation, and Resonate never retries it:
   * the loop below follows Mastra's retry configuration, sleeping durably
   * between attempts. A failed attempt is recorded, so a replay doesn't run it again.
   */
  override async executeStepWithRetry<T>(
    stepId: string,
    runStep: () => Promise<T>,
    params: { retries: number; delay: number; stepSpan?: any; workflowId: string; runId: string },
  ): Promise<
    | { ok: true; result: T }
    | { ok: false; error: { status: 'failed'; error: Error; endedAt: number; nonRetryable?: true } }
  > {
    type Attempt = { ok: true; result: T } | { ok: false; error: unknown; nonRetryable: boolean };
    for (let i = 0; i <= params.retries; i++) {
      if (i > 0 && params.delay > 0) {
        await this.#sleep({ for: params.delay });
      }
      const attempt = await durable<Attempt>(`${stepId}#attempt${i}`, this.#config.timeout, () =>
        retryCountStorage.run(i, async () => {
          try {
            return { ok: true as const, result: await runStep() };
          } catch (e) {
            return {
              ok: false as const,
              error: encodeError(e),
              nonRetryable: isNonRetryableStepFailure(e),
            };
          }
        }),
      );
      if (attempt.ok) return { ok: true, result: attempt.result };
      if (attempt.nonRetryable || i === params.retries) {
        const error = decodeError(attempt.error);
        params.stepSpan?.error({ error, attributes: { status: 'failed' } });
        return {
          ok: false,
          error: {
            status: 'failed',
            error,
            endedAt: Date.now(),
            ...(attempt.nonRetryable && { nonRetryable: true as const }),
          },
        };
      }
    }
    return {
      ok: false,
      error: { status: 'failed', error: new Error('Unknown error'), endedAt: Date.now() },
    };
  }

  override async executeSleepDuration(duration: number, _sleepId: string, _workflowId: string): Promise<void> {
    await this.#sleep({ for: Math.max(0, duration) });
  }

  override async executeSleepUntilDate(date: Date, _sleepUntilId: string, _workflowId: string): Promise<void> {
    await this.#sleep({ until: date });
  }

  async #sleep(opts: { for: number } | { until: Date }): Promise<void> {
    const ctx = currentContext();
    // A pass replays from the top after each sleep, so the wake time of a duration
    // is recorded once: every replay checks and sleeps until the same instant.
    const wakeAt =
      'until' in opts
        ? opts.until.getTime()
        : await durable('sleep.wakeAt', this.#config.timeout, async () => Date.now() + opts.for, ctx);
    if (wakeAt > ctx.timeoutAt) {
      throw new MastraNonRetryableError({
        id: 'RESONATE_ENGINE_SLEEP_PAST_DEADLINE',
        domain: 'MASTRA_WORKFLOW',
        category: 'USER',
        text:
          `A sleep until ${new Date(wakeAt).toISOString()} ends after the run's durable deadline ` +
          `(${new Date(ctx.timeoutAt).toISOString()}). Raise the engine's timeout in init(resonate, { timeout }).`,
      } as any);
    }
    try {
      await ctx.sleep({ until: new Date(wakeAt) });
    } catch (e) {
      if (isPassOver(e)) return parked();
      throw e;
    }
  }

  /** The run's suspended paths, kept to order the result's `suspended` list. */
  #suspendedPaths: Record<string, number[]> = {};

  override applyMutableContext(
    executionContext: ExecutionContext,
    mutableContext: Parameters<DefaultExecutionEngine['applyMutableContext']>[1],
  ): void {
    super.applyMutableContext(executionContext, mutableContext);
    this.#suspendedPaths = executionContext.suspendedPaths;
  }

  /**
   * Concurrent branches finish in network order, and core lists suspended steps in
   * the order their results arrived. Order them by execution path (graph order)
   * instead, which is what the default engine, finishing them in-process, produces.
   */
  protected override async fmtReturnValue<TOutput>(
    ...args: Parameters<DefaultExecutionEngine['fmtReturnValue']>
  ): Promise<TOutput> {
    const result = (await super.fmtReturnValue(...args)) as { suspended?: string[][] };
    const paths = this.#suspendedPaths;
    if (Array.isArray(result?.suspended) && result.suspended.length > 1) {
      result.suspended = [...result.suspended].sort((a, b) => comparePaths(paths[a[0] ?? ''], paths[b[0] ?? '']));
    }
    return result as TOutput;
  }

  // Step executions: one child context each, so that concurrent branches
  // (parallel, conditional, foreach) never interleave durable operations.

  override executeStep(params: ExecuteStepParams): Promise<StepExecutionResult> {
    return this.#inChild(entryLabel('step', params.step.id, params.executionContext), () => super.executeStep(params));
  }

  override executeAgent(params: ExecuteAgentParams): Promise<StepExecutionResult> {
    return this.#inChild(entryLabel('agent', entryId(params.entry), params.executionContext), () =>
      super.executeAgent(params),
    );
  }

  override executeTool(params: ExecuteToolParams): Promise<StepExecutionResult> {
    return this.#inChild(entryLabel('tool', entryId(params.entry), params.executionContext), () =>
      super.executeTool(params),
    );
  }

  override executeClassifier(params: ExecuteClassifierParams): Promise<StepExecutionResult> {
    return this.#inChild(entryLabel('classifier', entryId(params.entry), params.executionContext), () =>
      super.executeClassifier(params),
    );
  }

  override executeMapping(params: ExecuteMappingParams): Promise<StepExecutionResult> {
    return this.#inChild(entryLabel('mapping', entryId(params.entry), params.executionContext), () =>
      super.executeMapping(params),
    );
  }

  /**
   * The child is created synchronously, in the caller's turn, so siblings started
   * together by a parallel or foreach block take their lineage positions in order.
   */
  #inChild<T>(label: string, fn: () => Promise<T>): Promise<T> {
    return durable(label, this.#config.timeout, fn);
  }

  /** Lifecycle callbacks run once, in the run's memoised finalize operation (see ResonateWorkflow). */
  public override async invokeLifecycleCallbacks(
    _result: Parameters<DefaultExecutionEngine['invokeLifecycleCallbacks']>[0],
  ): Promise<void> {}

  public async invokeLifecycleCallbacksInternal(
    result: Parameters<DefaultExecutionEngine['invokeLifecycleCallbacks']>[0],
  ): Promise<void> {
    return super.invokeLifecycleCallbacks(result);
  }

  /**
   * A nested `ResonateWorkflow` runs as a durable child of the step's context,
   * under the parent's run id (as the default engine does), with its own snapshot.
   */
  override async executeWorkflowStep(
    params: ExecuteWorkflowStepParams,
  ): Promise<StepResult<any, any, any, any> | null> {
    if (!(params.step instanceof ResonateWorkflow)) return null;

    const {
      step,
      stepResults,
      executionContext,
      resume,
      timeTravel,
      prevOutput,
      inputData,
      pubsub,
      startedAt,
      perStep,
      actor,
    } = params;
    if (timeTravel && timeTravel.steps?.length > 1 && timeTravel.steps[0] === step.id) {
      throw new MastraNonRetryableError({
        id: 'RESONATE_ENGINE_TIME_TRAVEL_UNSUPPORTED',
        domain: 'MASTRA_WORKFLOW',
        category: 'USER',
        text: 'Time travel is not supported by @mastra/resonate yet.',
      } as any);
    }

    const requestContext = params.requestContext as RequestContext | undefined;
    // Nested runs use the parent's run id, as the default engine does, with a
    // per-index suffix inside foreach so concurrent iterations keep separate snapshots.
    const derivedRunId =
      executionContext.foreachIndex !== undefined
        ? `${executionContext.runId}-foreach-${executionContext.foreachIndex}`
        : executionContext.runId;
    const isResume = !!resume?.steps?.length && resume.steps[0] === step.id;
    // Inside a foreach every iteration sees the aggregate step result, whose
    // __workflow_meta names the first suspended iteration's run; use this iteration's.
    const runId: string =
      isResume && executionContext.foreachIndex === undefined
        ? ((stepResults[step.id] as any)?.suspendPayload?.__workflow_meta?.runId ?? derivedRunId)
        : derivedRunId;

    let nestedResume: InvocationPayload['resume'];
    if (isResume) {
      // Which nested step to resume: the rest of the caller's path, or the
      // nested run's only suspended step.
      nestedResume = await this.wrapDurableOperation(
        `workflow.${executionContext.workflowId}.step.${step.id}.nested-resume`,
        async () => {
          const store = await this.mastra?.getStorage()?.getStore('workflows');
          const snapshot: any = await store?.loadWorkflowSnapshot({ workflowName: step.id, runId });
          const nestedSteps = resume!.steps.slice(1);
          // Normally resolved by the top-level pass; resolve it here the same way if not.
          if (nestedSteps.length === 0 && snapshot) {
            nestedSteps.push(...resolveResumeSteps(snapshot, {}));
          }
          return {
            steps: nestedSteps,
            resumePayload: resume!.resumePayload,
            resumePath: nestedSteps[0] ? snapshot?.suspendedPaths?.[nestedSteps[0]] : undefined,
          };
        },
      );
    }

    const payload: InvocationPayload = {
      kind: isResume ? 'resume' : 'start',
      role: 'pass',
      workflowId: step.id,
      runId,
      resourceId: undefined,
      inputData,
      initialState: executionContext.state ?? {},
      requestContext: requestContext ? this.serializeRequestContext(requestContext) : {},
      outputOptions: { includeState: true, includeResumeLabels: true },
      perStep,
      actor,
      ...(nestedResume ? { resume: nestedResume } : {}),
      nested: {
        compact: true,
        tracing: executionContext.tracingIds?.traceId
          ? { traceId: executionContext.tracingIds.traceId, parentSpanId: params.stepSpan?.id }
          : undefined,
      },
    };

    const nested = step as ResonateWorkflow;
    const result: any = await durable(
      `workflow:${step.id}@${executionContext.executionPath.join('.')}`,
      this.#config.timeout,
      child => nested.runInvocation(child, payload),
    );
    executionContext.state = result.state ?? executionContext.state;

    const res = await this.wrapDurableOperation(
      `workflow.${executionContext.workflowId}.step.${step.id}.nestedwf-results`,
      async () => {
        const topic = `workflow.events.v2.${executionContext.runId}`;
        if (result.status === 'failed') {
          await pubsub.publish(topic, {
            type: 'watch',
            runId: executionContext.runId,
            data: {
              type: 'workflow-step-result',
              payload: { id: step.id, status: 'failed', error: result.error, payload: prevOutput },
            },
          });
          return {
            executionContext,
            result: { status: 'failed', error: result.error, endedAt: Date.now() },
          };
        }
        if (result.status === 'suspended') {
          const suspendedSteps = Object.entries(result.steps ?? {}).filter(
            ([, stepResult]) => (stepResult as StepResult<any, any, any, any>)?.status === 'suspended',
          );
          for (const [stepName, stepResult] of suspendedSteps) {
            const nestedMeta = (stepResult as any)?.suspendPayload?.__workflow_meta ?? {};
            const suspendPath: string[] = [stepName, ...(nestedMeta.path ?? [])];
            executionContext.suspendedPaths[step.id] = executionContext.executionPath;
            for (const label of Object.keys(result.resumeLabels ?? {})) {
              executionContext.resumeLabels[label] = { stepId: step.id };
            }
            await pubsub.publish(topic, {
              type: 'watch',
              runId: executionContext.runId,
              data: {
                type: 'workflow-step-suspended',
                payload: { id: step.id, status: 'suspended' },
              },
            });
            return {
              executionContext,
              result: {
                status: 'suspended',
                suspendedAt: Date.now(),
                payload: (stepResult as any).payload,
                suspendPayload: {
                  ...(stepResult as any)?.suspendPayload,
                  __workflow_meta: { ...nestedMeta, runId, path: suspendPath },
                },
              },
            };
          }
          return {
            executionContext,
            result: { status: 'suspended', suspendedAt: Date.now(), payload: {} },
          };
        }
        if (result.status === 'tripwire') {
          await pubsub.publish(topic, {
            type: 'watch',
            runId: executionContext.runId,
            data: {
              type: 'workflow-step-result',
              payload: {
                id: step.id,
                status: 'tripwire',
                error: result.tripwire?.reason,
                payload: prevOutput,
              },
            },
          });
          return {
            executionContext,
            result: { status: 'tripwire', tripwire: result.tripwire, endedAt: Date.now() },
          };
        }
        if (perStep || result.status === 'paused') {
          await pubsub.publish(topic, {
            type: 'watch',
            runId: executionContext.runId,
            data: { type: 'workflow-step-result', payload: { id: step.id, status: 'paused' } },
          });
          await pubsub.publish(topic, {
            type: 'watch',
            runId: executionContext.runId,
            data: { type: 'workflow-step-finish', payload: { id: step.id, metadata: {} } },
          });
          return { executionContext, result: { status: 'paused' } };
        }
        await pubsub.publish(topic, {
          type: 'watch',
          runId: executionContext.runId,
          data: {
            type: 'workflow-step-result',
            payload: { id: step.id, status: 'success', output: result.result },
          },
        });
        await pubsub.publish(topic, {
          type: 'watch',
          runId: executionContext.runId,
          data: { type: 'workflow-step-finish', payload: { id: step.id, metadata: {} } },
        });
        return {
          executionContext,
          result: { status: 'success', output: result.result, endedAt: Date.now() },
        };
      },
    );

    Object.assign(executionContext, res.executionContext);
    return {
      ...res.result,
      startedAt,
      payload: inputData,
      resumedAt: isResume ? startedAt : undefined,
      resumePayload: isResume ? resume?.resumePayload : undefined,
    } as StepResult<any, any, any, any>;
  }
}

function comparePaths(a: number[] = [], b: number[] = []): number {
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    const d = (a[i] ?? 0) - (b[i] ?? 0);
    if (d !== 0) return d;
  }
  return a.length - b.length;
}

function entryId(entry: { id?: string; type?: string; step?: { id?: string } } | undefined): string {
  return entry?.id ?? entry?.step?.id ?? entry?.type ?? 'entry';
}

export type { InvocationPayload };
