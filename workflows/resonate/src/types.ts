import type { Step, WorkflowConfig, WorkflowRunStatus } from '@mastra/core/workflows';
import type { Context } from '@resonatehq/sdk/async';

/** The engine primitives a step can read from `engine` in its execute function. */
export type ResonateEngineType = {
  /** The Resonate context of the current durable operation. */
  ctx: Context;
  /** The id of the run's root durable promise. */
  rootId: string;
};

/** Options for {@link init}. */
export interface ResonateEngineConfig {
  /**
   * Deadline of a run's durable promises, in milliseconds from when the run is
   * dispatched. Every sleep, sleep-until and suspension must end before it.
   * @default 30 days
   */
  timeout?: number;
  /**
   * The worker group passes are dispatched to. It must be the `group` the
   * Resonate client was created with; when omitted it is read from the client
   * and falls back to Resonate's default group, `default`.
   */
  group?: string;
}

export const DEFAULT_RUN_TIMEOUT_MS = 30 * 24 * 60 * 60 * 1000;

export type ResolvedEngineConfig = Required<ResonateEngineConfig>;

export function resolveConfig(config: ResonateEngineConfig = {}, detectedGroup?: string): ResolvedEngineConfig {
  const timeout = config.timeout ?? DEFAULT_RUN_TIMEOUT_MS;
  if (!Number.isFinite(timeout) || timeout <= 0) {
    throw new Error(`@mastra/resonate: timeout must be a positive number of milliseconds, got ${timeout}`);
  }
  const group = config.group ?? detectedGroup ?? 'default';
  if (detectedGroup !== undefined && group !== detectedGroup) {
    throw new Error(
      `@mastra/resonate: group "${group}" differs from the Resonate client's group "${detectedGroup}"; ` +
        'passes would be dispatched to workers that do not run this client',
    );
  }
  return { timeout, group };
}

/**
 * Reads the worker group from a Resonate client's poll address
 * (`poll://any@<group>/<pid>`), when the client exposes it.
 */
export function detectGroup(resonate: unknown): string | undefined {
  const anycast = (resonate as { network?: { anycast?: unknown } } | undefined)?.network?.anycast;
  if (typeof anycast !== 'string') return undefined;
  const match = /^poll:\/\/any@([^/]+)/.exec(anycast);
  return match?.[1] ? decodeURIComponent(match[1]) : undefined;
}

export type ResonateWorkflowConfig<
  TWorkflowId extends string,
  TState,
  TInput,
  TOutput,
  TSteps extends Step<string, any, any, any, any, any, ResonateEngineType, any>[],
  TRequestContext extends Record<string, any> | unknown = unknown,
> = WorkflowConfig<TWorkflowId, TState, TInput, TOutput, TSteps, TRequestContext>;

/**
 * What a run's root durable function receives. One shape for the first
 * invocation and for nested workflows; resume and time travel add fields.
 */
/**
 * The value that settles a run's resume promise. `run.resume()` writes it;
 * any Resonate client may write one to resume a run from outside Mastra.
 */
export interface ResumeRequest {
  /** Unique per resume call; tells the caller whose request settled the promise. */
  resumeId: string;
  resumeData?: unknown;
  /** A step id, a dotted path or a list of step ids into nested workflows. */
  step?: string | string[];
  /**
   * The `suspendedAt` of the suspension this request answers, from the snapshot the
   * caller read. When set, a request for a suspension that was already resumed (the
   * step has since suspended again, or not at all) is rejected.
   */
  suspendedAt?: number;
  label?: string;
  requestContext?: Record<string, unknown>;
  actor?: unknown;
  perStep?: boolean;
  forEachIndex?: number;
}

export interface InvocationPayload {
  kind: 'start' | 'resume';
  /**
   * `root` is the run's root promise, which dispatches each pass and waits for
   * it; `pass` executes the graph. Nested workflows run as `pass` directly.
   */
  role: 'root' | 'pass';
  workflowId: string;
  runId: string;
  resourceId?: string;
  inputData?: unknown;
  initialState?: unknown;
  /** Serialized request context entries (never the auth token). */
  requestContext: Record<string, unknown>;
  outputOptions?: { includeState?: boolean; includeResumeLabels?: boolean };
  tracingOptions?: unknown;
  format?: 'legacy' | 'vnext';
  perStep?: boolean;
  actor?: unknown;
  /** A resume of the top-level run: the request that settled its resume promise. */
  resumeRequest?: ResumeRequest;
  /** A resume of a nested run, resolved by the parent. */
  resume?: { steps: string[]; resumePayload: unknown; resumePath?: number[] };
  /** Set when a parent workflow invokes this one as a nested step. */
  nested?: { compact: boolean; tracing?: { traceId: string; parentSpanId?: string } };
}

/**
 * The Resonate root id of a Mastra run. Resonate reserves `:` (it separates a
 * promise's origin from its lineage), so `%` and `:` are percent-encoded.
 */
export function rootIdForRun(runId: string): string {
  if (!runId) throw new Error('@mastra/resonate: runId must not be empty');
  return runId.replace(/%/g, '%25').replace(/:/g, '%3A');
}

/**
 * A run's root promise alternates two kinds of children: pass `k` at
 * `<root>:2k` (a global promise, the run's k-th execution) and, after a pass
 * that suspended, the resume promise at `<root>:2k+1`.
 */
export function passId(rootId: string, k: number): string {
  return `${rootId}:${2 * k}`;
}

export function resumePromiseId(rootId: string, k: number): string {
  return `${rootId}:${2 * k + 1}`;
}

/** The durable promise that holds a run's first pass (the root's first child). */
export function firstPassId(rootId: string): string {
  return passId(rootId, 0);
}

/** Resonate durable promise states at protocol 2026-04-01 (`@resonatehq/sdk` 0.11.5). */
export type DurablePromiseState = 'pending' | 'resolved' | 'rejected' | 'rejected_canceled' | 'rejected_timedout';

/**
 * Maps the state of a run's root durable promise to a Mastra run status. A
 * pending root is running from Resonate's point of view; whether it is
 * suspended or waiting is in the Mastra snapshot.
 */
export function statusFromPromiseState(state: DurablePromiseState): WorkflowRunStatus {
  switch (state) {
    case 'pending':
      return 'running';
    case 'resolved':
      return 'success';
    case 'rejected':
    case 'rejected_timedout':
      return 'failed';
    case 'rejected_canceled':
      return 'canceled';
  }
}
