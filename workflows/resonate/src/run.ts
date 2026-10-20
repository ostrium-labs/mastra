import { getErrorFromUnknown, MastraError } from '@mastra/core/error';
import type { Mastra } from '@mastra/core/mastra';
import { MASTRA_AUTH_TOKEN_KEY, RequestContext } from '@mastra/core/request-context';
import type { WorkflowResult } from '@mastra/core/workflows';
import { hydrateSerializedStepErrors, Run } from '@mastra/core/workflows';
import { Codec } from '@resonatehq/sdk';
import type { Resonate, ResonateHandle } from '@resonatehq/sdk/async';
import { decodeError, decodeValue, encodeValue } from './codec';
import { resolveResumeSteps } from './resume';
import type {
  DurablePromiseState,
  InvocationPayload,
  ResolvedEngineConfig,
  ResonateEngineType,
  ResumeRequest,
} from './types';
import { firstPassId, passId, resumePromiseId, rootIdForRun, statusFromPromiseState } from './types';

type RunParams = ConstructorParameters<typeof Run>[0];

/** Serializes a request context for a durable payload, without the framework's bearer token. */
export function serializeRequestContext(requestContext?: RequestContext<any>): Record<string, unknown> {
  const obj = requestContext ? requestContext.toJSON() : {};
  delete obj[MASTRA_AUTH_TOKEN_KEY];
  return obj;
}

function notSupported(what: string): MastraError {
  return new MastraError({
    id: 'RESONATE_ENGINE_UNSUPPORTED',
    domain: 'MASTRA_WORKFLOW',
    category: 'USER',
    text: `${what} is not supported by @mastra/resonate yet.`,
  } as any);
}

function isNotFound(error: unknown): boolean {
  const e = error as { serverError?: { code?: number }; code?: unknown } | undefined;
  return e?.serverError?.code === 404;
}

export class ResonateRun<
  TEngineType = ResonateEngineType,
  TSteps extends any[] = any[],
  TState = unknown,
  TInput = unknown,
  TOutput = unknown,
  TRequestContext = unknown,
> extends Run<TEngineType, TSteps, TState, TInput, TOutput, TRequestContext> {
  readonly #resonate: Resonate;
  readonly #config: ResolvedEngineConfig;
  readonly #mastra: Mastra | undefined;

  constructor(params: RunParams, resonate: Resonate, config: ResolvedEngineConfig) {
    super(params as any);
    this.#resonate = resonate;
    this.#config = config;
    this.#mastra = params.mastra;
  }

  /** The id of this run's root durable promise on the Resonate server. */
  get rootId(): string {
    return rootIdForRun(this.runId);
  }

  /** The run's status as the Resonate server sees its root durable promise. */
  async durableStatus() {
    const record = await this.#resonate.promises.get(this.rootId);
    return statusFromPromiseState(record.state as DurablePromiseState);
  }

  protected override async _start(
    args: {
      inputData?: TInput;
      initialState?: TState;
      requestContext?: RequestContext<TRequestContext>;
      tracingOptions?: unknown;
      format?: 'legacy' | 'vnext';
      outputOptions?: { includeState?: boolean; includeResumeLabels?: boolean };
      perStep?: boolean;
      actor?: unknown;
    },
    onDispatched?: () => void,
  ): Promise<WorkflowResult<TState, TInput, TOutput, any>> {
    const { inputData, initialState, requestContext, tracingOptions, format, outputOptions, perStep, actor } = args;

    const inputDataToUse = await this._validateInput(inputData);
    const initialStateToUse = await this._validateInitialState(initialState ?? ({} as TState));
    await this._validateRequestContext(requestContext as RequestContext | undefined);

    // onStart is a pre-flight gate, as in the default engine: it runs once, here,
    // before anything is dispatched, and its error rejects the caller.
    await this.executionEngine.invokeStartCallback({
      runId: this.runId,
      workflowId: this.workflowId,
      resourceId: this.resourceId,
      getInitData: () => inputDataToUse,
      requestContext: (requestContext ?? new RequestContext()) as RequestContext,
      state: initialStateToUse as Record<string, any>,
    });

    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    const persistRunning = this.executionEngine.options.shouldPersistSnapshot({
      workflowStatus: 'running',
      stepResults: {},
    });
    if (persistRunning)
      await workflowsStore?.persistWorkflowSnapshot({
        workflowName: this.workflowId,
        runId: this.runId,
        resourceId: this.resourceId,
        snapshot: {
          runId: this.runId,
          serializedStepGraph: this.serializedStepGraph,
          status: 'running',
          value: {},
          context: {} as any,
          activePaths: [],
          suspendedPaths: {},
          activeStepsPath: {},
          resumeLabels: {},
          waitingPaths: {},
          timestamp: Date.now(),
        },
      });

    const payload: InvocationPayload = {
      kind: 'start',
      role: 'root',
      workflowId: this.workflowId,
      runId: this.runId,
      resourceId: this.resourceId,
      inputData: inputDataToUse,
      initialState: initialStateToUse,
      requestContext: serializeRequestContext(requestContext as RequestContext | undefined),
      outputOptions,
      tracingOptions,
      format,
      perStep,
      actor,
    };

    const root = await this.#resonate.run(
      this.rootId,
      this.workflowId,
      encodeValue(payload),
      this.#resonate.options({ timeout: this.#config.timeout }),
    );
    this.workflowRunStatus = 'running';
    onDispatched?.();

    const result = (await this.#awaitPass(root, firstPassId(this.rootId))) as WorkflowResult<
      TState,
      TInput,
      TOutput,
      any
    >;
    hydrateFailedResult(result);
    if (!outputOptions?.includeState) {
      delete (result as { state?: unknown }).state;
    }
    this.workflowRunStatus = result.status;
    if (result.status !== 'suspended') {
      this.cleanup?.();
    }
    return result;
  }

  /**
   * Waits for one pass of the run. The pass is the root's child at `passId`;
   * it may not exist yet when this is called, and the root may be canceled or
   * time out before it settles, so both are watched.
   */
  async #awaitPass(root: ResonateHandle<unknown>, passId: string): Promise<unknown> {
    let rootSettled: { error: unknown } | undefined;
    const rootDone = root.result().then(
      () => undefined,
      error => {
        rootSettled = { error };
      },
    );

    for (let delay = 10; ; delay = Math.min(delay * 2, 250)) {
      let pass: ResonateHandle<unknown> | undefined;
      try {
        pass = await this.#resonate.get(passId);
      } catch (e) {
        if (!isNotFound(e)) throw e;
      }
      if (pass) {
        try {
          return decodeValue(await Promise.race([pass.result(), rootDone.then(() => pass!.result())]));
        } catch (e) {
          throw decodeError(e);
        }
      }
      await Promise.race([rootDone, new Promise(resolve => setTimeout(resolve, delay))]);
      if (rootSettled) {
        throw getErrorFromUnknown(rootSettled.error, {
          fallbackMessage: `Run ${this.runId} ended before its first pass`,
        });
      }
    }
  }

  /**
   * Cancels the run: its pending pass (sleeping or running) and resume promise,
   * then the root. A sleeping pass never wakes, and a running one can settle
   * nothing more once its promise is canceled. The snapshot is marked canceled.
   */
  override async cancel(): Promise<void> {
    const ids: string[] = [];
    for (let k = 0; ; k++) {
      const pass = await this.#stateOf(passId(this.rootId, k));
      if (pass === undefined) break;
      if (pass === 'pending') ids.push(passId(this.rootId, k));
      const wait = await this.#stateOf(resumePromiseId(this.rootId, k));
      if (wait === undefined) break;
      if (wait === 'pending') ids.push(resumePromiseId(this.rootId, k));
    }
    ids.push(this.rootId);
    for (const id of ids) {
      try {
        await this.#resonate.promises.cancel(id);
      } catch (e) {
        // Settled in the meantime, or never dispatched: nothing left to stop there.
        if (!isNotFound(e)) {
          this.#mastra?.getLogger()?.debug?.(`Cancel of ${id} was refused`, e);
        }
      }
    }

    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    const snapshot = await workflowsStore?.loadWorkflowSnapshot({
      workflowName: this.workflowId,
      runId: this.runId,
    });
    if (snapshot) {
      await workflowsStore?.persistWorkflowSnapshot({
        workflowName: this.workflowId,
        runId: this.runId,
        resourceId: this.resourceId,
        snapshot: { ...snapshot, status: 'canceled' as any },
      });
    }
    this.workflowRunStatus = 'canceled';
  }

  async #stateOf(id: string): Promise<DurablePromiseState | undefined> {
    try {
      return (await this.#resonate.promises.get(id)).state as DurablePromiseState;
    } catch (e) {
      if (isNotFound(e)) return undefined;
      throw e;
    }
  }

  /**
   * Resumes a suspended run by settling its resume promise with a
   * {@link ResumeRequest}, then waits for the pass that the settlement starts.
   * The promise settles once: a concurrent resume of the same suspension is
   * refused.
   */
  protected override async _resume(params: {
    resumeData?: unknown;
    step?: any;
    label?: string;
    requestContext?: RequestContext<any>;
    outputOptions?: { includeState?: boolean; includeResumeLabels?: boolean };
    forEachIndex?: number;
    perStep?: boolean;
    actor?: unknown;
  }): Promise<WorkflowResult<TState, TInput, TOutput, any>> {
    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    if (!workflowsStore) {
      throw new Error(`Workflow storage is required to resume run ${this.runId}`);
    }
    const snapshot = await workflowsStore.loadWorkflowSnapshot({
      workflowName: this.workflowId,
      runId: this.runId,
    });
    if (!snapshot) {
      throw new Error(`No snapshot found for this workflow run: ${this.workflowId} ${this.runId}`);
    }

    // Resolve and validate here, so the caller gets core's errors before anything is settled.
    const steps = resolveResumeSteps(snapshot, { step: params.step, label: params.label });
    // Validated here for an early error; the pass validates and transforms the raw
    // data once, as it does for a request from a client outside Mastra.
    await this._validateResumeData(params.resumeData, this.workflowSteps[steps[0] ?? '']);

    const request: ResumeRequest = {
      resumeId: globalThis.crypto.randomUUID(),
      resumeData: params.resumeData,
      step: steps,
      // Names the suspension this caller saw, so a late request cannot resume a later one.
      suspendedAt: (snapshot.context as Record<string, any>)?.[steps[0] ?? '']?.suspendedAt,
      ...(params.label ? { label: params.label } : {}),
      requestContext: serializeRequestContext(params.requestContext),
      ...(params.actor !== undefined ? { actor: params.actor } : {}),
      ...(params.perStep !== undefined ? { perStep: params.perStep } : {}),
      ...(params.forEachIndex !== undefined ? { forEachIndex: params.forEachIndex } : {}),
    };

    const k = await this.#pendingResume();
    const settled = await this.#resonate.promises.resolve(
      resumePromiseId(this.rootId, k),
      new Codec().encode(encodeValue(request)),
    );
    const winner = decodeValue<ResumeRequest | undefined>(new Codec().decode(settled.value as any));
    if (winner?.resumeId !== request.resumeId) {
      throw new MastraError({
        id: 'RESONATE_ENGINE_ALREADY_RESUMED',
        domain: 'MASTRA_WORKFLOW',
        category: 'USER',
        text: `Run ${this.runId} was already resumed by another caller`,
        details: { workflowId: this.workflowId, runId: this.runId },
      } as any);
    }

    this.workflowRunStatus = 'running';
    const root = await this.#resonate.get(this.rootId);
    const result = (await this.#awaitPass(root, passId(this.rootId, k + 1))) as WorkflowResult<
      TState,
      TInput,
      TOutput,
      any
    > & {
      resumeRejected?: unknown;
    };
    if (result.resumeRejected !== undefined) {
      throw decodeError(result.resumeRejected);
    }
    hydrateFailedResult(result);
    if (!params.outputOptions?.includeState) {
      delete (result as { state?: unknown }).state;
    }
    this.workflowRunStatus = result.status;
    if (result.status !== 'suspended') {
      this.cleanup?.();
    }
    return result;
  }

  /**
   * Finds `k` such that the run's resume promise `<root>:2k+1` is pending.
   * Right after a pass suspends, the root has not created that promise yet,
   * so a missing promise is waited for while the root is still pending.
   */
  async #pendingResume(waitMs = 30_000): Promise<number> {
    const deadline = Date.now() + waitMs;
    for (let k = 0, delay = 10; ;) {
      try {
        const record = await this.#resonate.promises.get(resumePromiseId(this.rootId, k));
        if (record.state === 'pending') return k;
        k++;
        continue;
      } catch (e) {
        if (!isNotFound(e)) throw e;
      }
      const root = await this.#resonate.promises.get(this.rootId);
      if (root.state !== 'pending' || Date.now() > deadline) {
        throw new MastraError({
          id: 'RESONATE_ENGINE_NOT_AWAITING_RESUME',
          domain: 'MASTRA_WORKFLOW',
          category: 'USER',
          text: `Run ${this.runId} is not waiting to be resumed (root promise ${root.state})`,
          details: { workflowId: this.workflowId, runId: this.runId },
        } as any);
      }
      await new Promise(resolve => setTimeout(resolve, delay));
      delay = Math.min(delay * 2, 250);
    }
  }

  protected override async _timeTravel(): Promise<never> {
    throw notSupported('Time travel');
  }

  protected override async _restart(): Promise<never> {
    throw notSupported('Restart');
  }
}

function hydrateFailedResult(result: WorkflowResult<any, any, any, any>): void {
  if (result.status === 'failed') {
    result.error = getErrorFromUnknown(result.error, { serializeStack: false });
    if (result.steps) hydrateSerializedStepErrors(result.steps);
  }
}
