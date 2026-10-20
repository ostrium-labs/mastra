import { ErrorCategory, ErrorDomain, MastraError } from '@mastra/core/error';
import type { WorkflowRunState } from '@mastra/core/workflows';

type StepRef = string | { id: string };

/** What a resume names: a step (or a path into nested workflows), or a resume label. */
export interface ResumeTarget {
  step?: StepRef | StepRef[];
  label?: string;
}

/**
 * Resolves the step path a resume addresses, as core's `Run._resume` does: a
 * label wins, then an explicit step or path, else the only suspended step
 * (with its nested path). Throws the same errors core throws.
 */
export function resolveResumeSteps(snapshot: WorkflowRunState, target: ResumeTarget): string[] {
  if (snapshot.status !== 'suspended') {
    throw new MastraError({
      id: 'WORKFLOW_RUN_NOT_SUSPENDED',
      domain: ErrorDomain.MASTRA_WORKFLOW,
      category: ErrorCategory.USER,
      text: 'This workflow run was not suspended',
      details: { runId: snapshot.runId, status: snapshot.status },
    });
  }

  const labelled = target.label ? snapshot.resumeLabels?.[target.label] : undefined;
  const stepParam = labelled?.stepId ?? target.step;

  let steps: string[];
  if (stepParam) {
    const list =
      typeof stepParam === 'string' ? stepParam.split('.') : Array.isArray(stepParam) ? stepParam : [stepParam];
    steps = list.map(step => (typeof step === 'string' ? step : step.id));
  } else {
    const suspendedStepPaths: string[][] = [];
    for (const stepId of Object.keys(snapshot.suspendedPaths ?? {})) {
      const stepResult = (snapshot.context as Record<string, any> | undefined)?.[stepId];
      if (stepResult && typeof stepResult === 'object' && stepResult.status === 'suspended') {
        const nestedPath = stepResult.suspendPayload?.__workflow_meta?.path;
        suspendedStepPaths.push(Array.isArray(nestedPath) ? [stepId, ...nestedPath] : [stepId]);
      }
    }
    if (suspendedStepPaths.length === 0) {
      throw new Error('No suspended steps found in this workflow run');
    }
    if (suspendedStepPaths.length > 1) {
      const pathStrings = suspendedStepPaths.map(path => `[${path.join(', ')}]`);
      throw new Error(
        `Multiple suspended steps found: ${pathStrings.join(', ')}. ` +
          'Please specify which step to resume using the "step" parameter.',
      );
    }
    steps = suspendedStepPaths[0]!;
  }

  const suspendedStepIds = Object.keys(snapshot.suspendedPaths ?? {});
  if (!suspendedStepIds.includes(steps[0] ?? '')) {
    throw new Error(
      `This workflow step "${steps[0]}" was not suspended. Available suspended steps: [${suspendedStepIds.join(', ')}]`,
    );
  }
  return steps;
}

/** Validates `value` against a Standard Schema, as core does for resume data. */
export async function validateWithSchema(schema: unknown, value: unknown, what: string): Promise<unknown> {
  const standard = (schema as { '~standard'?: { validate: (v: unknown) => unknown } } | undefined)?.['~standard'];
  if (!standard) return value;
  const result = (await standard.validate(value)) as {
    value?: unknown;
    issues?: { message: string; path?: unknown[] }[];
  };
  if (result.issues?.length) {
    const details = result.issues
      .map(
        issue =>
          `- ${(issue.path ?? []).map(p => (typeof p === 'object' && p ? (p as any).key : p)).join('.') || 'root'}: ${issue.message}`,
      )
      .join('\n');
    throw new Error(`Invalid ${what}: \n${details}`);
  }
  return result.value;
}
