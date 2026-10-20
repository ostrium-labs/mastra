import { afterEach, describe, expect, it } from 'vitest';
import { z } from 'zod';
import type { Harness } from './__tests__/helpers';
import { harness, hasServer } from './__tests__/helpers';

let h: Harness | undefined;
afterEach(async () => {
  await h?.stop();
  h = undefined;
});

const num = z.object({ value: z.number() });

function summarize(events: any[]) {
  return events.map(
    e => `${e.type}${e.payload?.id ? `:${e.payload.id}` : ''}${e.payload?.status ? `:${e.payload.status}` : ''}`,
  );
}

function counts(list: string[]) {
  const out: Record<string, number> = {};
  for (const item of list) out[item] = (out[item] ?? 0) + 1;
  return out;
}

describe.skipIf(!hasServer)('streaming and watch', () => {
  it('stream_emits_each_step_once', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const step = (id: string) =>
      createStep({
        id,
        inputSchema: num,
        outputSchema: num,
        execute: async ({ inputData }) => ({ value: inputData.value + 1 }),
      });
    const wf = createWorkflow({ id: 'streamed', inputSchema: num, outputSchema: num })
      .then(step('a'))
      .then(step('b'))
      .commit();
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    const output = run.stream({ inputData: { value: 0 } });
    const events: any[] = [];
    for await (const event of output.fullStream) events.push(event);
    const result = await output.result;
    expect(result.status).toBe('success');
    // Exactly what the default engine streams for this workflow.
    expect(summarize(events)).toEqual([
      'workflow-start',
      'workflow-step-start:a:running',
      'workflow-step-result:a:success',
      'workflow-step-start:b:running',
      'workflow-step-result:b:success',
      'workflow-finish',
    ]);
  });

  it('no_duplicate_events_after_replay', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const step = (id: string) =>
      createStep({
        id,
        inputSchema: num,
        outputSchema: num,
        execute: async ({ inputData }) => ({ value: inputData.value + 1 }),
      });
    // Each sleep ends a pass of the task; the next pass replays everything before it.
    const wf = createWorkflow({ id: 'replayed', inputSchema: num, outputSchema: num })
      .then(step('a'))
      .sleep(200)
      .then(step('b'))
      .sleep(200)
      .then(step('c'))
      .commit();
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    const output = run.stream({ inputData: { value: 0 } });
    const events: any[] = [];
    for await (const event of output.fullStream) events.push(event);
    expect((await output.result).status).toBe('success');
    const list = summarize(events);
    const dupes = Object.entries(counts(list)).filter(([, n]) => n > 1);
    expect(dupes).toEqual([]);
    for (const id of ['a', 'b', 'c']) expect(list).toContain(`workflow-step-result:${id}:success`);
  });

  it('watch_sees_suspend_and_resume', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const gate = createStep({
      id: 'gate',
      inputSchema: num,
      outputSchema: num,
      resumeSchema: z.object({ ok: z.boolean() }),
      execute: async ({ inputData, resumeData, suspend }) => (resumeData ? inputData : suspend({})),
    });
    const wf = createWorkflow({ id: 'watched', inputSchema: num, outputSchema: num }).then(gate).commit();
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    const events: any[] = [];
    const unwatch = (run as any).watch((event: any) => events.push(event));
    expect((await run.start({ inputData: { value: 1 } })).status).toBe('suspended');
    expect((await run.resume({ step: 'gate', resumeData: { ok: true } })).status).toBe('success');
    await new Promise(r => setTimeout(r, 100));
    unwatch();
    // Exactly what the default engine's watch reports for a suspend and a resume.
    expect(summarize(events)).toEqual([
      'workflow-step-start:gate:running',
      'workflow-step-suspended:gate:suspended',
      'workflow-step-start:gate:running',
      'workflow-step-result:gate:success',
      'workflow-step-finish:gate',
    ]);
  });
});
