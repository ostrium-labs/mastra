import { Mastra } from '@mastra/core/mastra';
import { LibSQLStore } from '@mastra/libsql';
import { Resonate } from '@resonatehq/sdk/async';
import type { ResonateEngineConfig } from '../index';
import { init } from '../index';

/** The Resonate server under test (`docker compose up -d` in `pretest` starts it). */
export const RESONATE_URL = process.env.RESONATE_URL ?? 'http://localhost:8001';
export const hasServer = process.env.RESONATE_SKIP_INTEGRATION !== '1';

export function uniqueId(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

export interface Harness {
  resonate: Resonate;
  group: string;
  createWorkflow: ReturnType<typeof init>['createWorkflow'];
  createStep: ReturnType<typeof init>['createStep'];
  mastraWith(workflows: Record<string, any>): Mastra;
  stop(): Promise<void>;
}

/** A Resonate client in its own group, plus init() bound to it. */
export function harness(config?: ResonateEngineConfig & { group?: string; storageUrl?: string }): Harness {
  const group = config?.group ?? uniqueId('g');
  const resonate = new Resonate({ url: RESONATE_URL, group, ttl: 5_000 });
  const { createWorkflow, createStep } = init(resonate, config);
  return {
    resonate,
    group,
    createWorkflow,
    createStep,
    mastraWith(workflows) {
      return new Mastra({
        workflows,
        storage: new LibSQLStore({ id: uniqueId('store'), url: config?.storageUrl ?? ':memory:' }),
        logger: false,
      });
    },
    stop: () => resonate.stop(),
  };
}
