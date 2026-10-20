import { Codec } from '@resonatehq/sdk';
import { Resonate } from '@resonatehq/sdk/async';
import { afterEach, describe, expect, it } from 'vitest';
import { z } from 'zod';
import type { Harness } from './__tests__/helpers';
import { harness, hasServer, RESONATE_URL } from './__tests__/helpers';
import { encodeValue } from './codec';

let h: Harness | undefined;
afterEach(async () => {
  await h?.stop();
  h = undefined;
});

const num = z.object({ value: z.number() });
const approval = z.object({ approved: z.boolean() });

function approvalWorkflow(h: Harness, id = 'approval') {
  const { createWorkflow, createStep } = h;
  const runs: Record<string, number> = {};
  const hit = (k: string) => (runs[k] = (runs[k] ?? 0) + 1);
  const prepare = createStep({
    id: 'prepare',
    inputSchema: num,
    outputSchema: num,
    execute: async ({ inputData }) => {
      hit('prepare');
      return { value: inputData.value + 1 };
    },
  });
  const approve = createStep({
    id: 'approve',
    inputSchema: num,
    outputSchema: num,
    resumeSchema: approval,
    suspendSchema: z.object({ reason: z.string() }),
    execute: async ({ inputData, resumeData, suspend }) => {
      hit('approve');
      if (!resumeData) {
        await suspend({ reason: 'needs approval' }, { resumeLabel: 'manager' });
        return { value: -1 };
      }
      return { value: resumeData.approved ? inputData.value * 100 : 0 };
    },
  });
  const wf = createWorkflow({ id, inputSchema: num, outputSchema: num }).then(prepare).then(approve).commit();
  return { wf, runs };
}

describe.skipIf(!hasServer)('suspend and resume', () => {
  it('suspend_then_resume', async () => {
    h = harness();
    const { wf, runs } = approvalWorkflow(h);
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();

    const first = await run.start({ inputData: { value: 1 } });
    expect(first.status).toBe('suspended');
    expect((first.steps.approve as any).suspendPayload).toMatchObject({ reason: 'needs approval' });
    const store = await mastra.getStorage()!.getStore('workflows');
    const snapshot = await store!.loadWorkflowSnapshot({
      workflowName: 'approval',
      runId: run.runId,
    });
    expect(snapshot?.status).toBe('suspended');
    expect(Object.keys(snapshot?.suspendedPaths ?? {})).toEqual(['approve']);
    expect(snapshot?.resumeLabels?.manager).toMatchObject({ stepId: 'approve' });

    const second = await run.resume({ step: 'approve', resumeData: { approved: true } });
    expect(second.status).toBe('success');
    if (second.status === 'success') expect(second.result).toEqual({ value: 200 });
    // prepare ran once; approve ran twice (suspended, then resumed).
    expect(runs).toEqual({ prepare: 1, approve: 2 });
  });

  it('resumes by label and rejects invalid resume data before settling anything', async () => {
    h = harness();
    const { wf } = approvalWorkflow(h);
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    expect((await run.start({ inputData: { value: 2 } })).status).toBe('suspended');

    await expect(run.resume({ label: 'manager', resumeData: { approved: 'yes' } as any })).rejects.toThrow(
      /resume data/i,
    );
    const result = await run.resume({ label: 'manager', resumeData: { approved: false } });
    expect(result.status).toBe('success');
    if (result.status === 'success') expect(result.result).toEqual({ value: 0 });
  });

  it('double_resume_is_refused', async () => {
    h = harness();
    const { wf, runs } = approvalWorkflow(h);
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    await run.start({ inputData: { value: 1 } });

    const again = await mastra.getWorkflow('wf').createRun({ runId: run.runId });
    const outcomes = await Promise.allSettled([
      run.resume({ step: 'approve', resumeData: { approved: true } }),
      again.resume({ step: 'approve', resumeData: { approved: false } }),
    ]);
    const fulfilled = outcomes.filter(o => o.status === 'fulfilled');
    const rejected = outcomes.filter(o => o.status === 'rejected') as PromiseRejectedResult[];
    expect(fulfilled).toHaveLength(1);
    expect(rejected).toHaveLength(1);
    expect(String(rejected[0]!.reason?.message ?? rejected[0]!.reason)).toMatch(
      /already resumed|not suspended|not waiting/,
    );
    expect(runs.approve).toBe(2);
  });

  it('resumes each of two parallel suspensions in turn', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const gate = (id: string) =>
      createStep({
        id,
        inputSchema: num,
        outputSchema: num,
        resumeSchema: approval,
        execute: async ({ inputData, resumeData, suspend }) => {
          if (!resumeData) return suspend({});
          return { value: inputData.value + (resumeData.approved ? 1 : 0) };
        },
      });
    const wf = createWorkflow({ id: 'two-gates', inputSchema: num, outputSchema: z.any() })
      .parallel([gate('left'), gate('right')])
      .commit();
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    expect((await run.start({ inputData: { value: 10 } })).status).toBe('suspended');
    await expect(run.resume({ resumeData: { approved: true } })).rejects.toThrow(/Multiple suspended steps/);
    expect((await run.resume({ step: 'left', resumeData: { approved: true } })).status).toBe('suspended');
    const done = await run.resume({ step: 'right', resumeData: { approved: true } });
    expect(done.status).toBe('success');
    if (done.status === 'success') expect(done.result).toEqual({ left: { value: 11 }, right: { value: 11 } });
  });

  it('nested_suspend_resume', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const { wf: child } = approvalWorkflow(h, 'child-approval');
    const finish = createStep({
      id: 'finish',
      inputSchema: num,
      outputSchema: num,
      execute: async ({ inputData }) => ({ value: inputData.value + 5 }),
    });
    const parent = createWorkflow({ id: 'parent', inputSchema: num, outputSchema: num })
      .then(child)
      .then(finish)
      .commit();
    const mastra = h.mastraWith({ parent });
    const run = await mastra.getWorkflow('parent').createRun();

    const first = await run.start({ inputData: { value: 1 } });
    expect(first.status).toBe('suspended');
    const second = await run.resume({
      step: ['child-approval', 'approve'],
      resumeData: { approved: true },
    });
    expect(second.status).toBe('success');
    if (second.status === 'success') expect(second.result).toEqual({ value: 205 });
  });

  it('is resumed by a Resonate client outside Mastra settling the resume promise', async () => {
    h = harness();
    const { wf } = approvalWorkflow(h);
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    expect((await run.start({ inputData: { value: 3 } })).status).toBe('suspended');

    // Any Resonate client can do this: find the run's pending resume promise and settle it.
    const outsider = new Resonate({ url: RESONATE_URL, group: `${h.group}-outsider` });
    try {
      const codec = new Codec();
      const resumeId = `${run.runId}:1`;
      for (let i = 0; i < 100; i++) {
        try {
          await outsider.promises.get(resumeId);
          break;
        } catch {
          await new Promise(r => setTimeout(r, 50));
        }
      }
      await outsider.promises.resolve(
        resumeId,
        codec.encode(encodeValue({ resumeId: 'outside', step: 'approve', resumeData: { approved: true } })),
      );
      const root = await outsider.get(run.runId);
      await root.result();
    } finally {
      await outsider.stop();
    }
    const store = await mastra.getStorage()!.getStore('workflows');
    const snapshot = await store!.loadWorkflowSnapshot({
      workflowName: 'approval',
      runId: run.runId,
    });
    expect(snapshot?.status).toBe('success');
    expect(snapshot?.result).toEqual({ value: 400 });
  });

  it('validates and transforms resume data once', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const gate = createStep({
      id: 'gate',
      inputSchema: num,
      outputSchema: num,
      resumeSchema: z.object({ n: z.string().transform(Number) }),
      execute: async ({ inputData, resumeData, suspend }) =>
        resumeData ? { value: inputData.value + resumeData.n } : suspend({}),
    });
    const wf = createWorkflow({ id: 'transform', inputSchema: num, outputSchema: num }).then(gate).commit();
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    expect((await run.start({ inputData: { value: 1 } })).status).toBe('suspended');
    const done = await run.resume({ step: 'gate', resumeData: { n: '5' } as any });
    expect(done.status).toBe('success');
    if (done.status === 'success') expect(done.result).toEqual({ value: 6 });
  });

  it('rejects an ambiguous nested target and keeps the run suspended', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const gate = (id: string) =>
      createStep({
        id,
        inputSchema: num,
        outputSchema: num,
        resumeSchema: approval,
        execute: async ({ inputData, resumeData, suspend }) => (resumeData ? inputData : suspend({})),
      });
    const child = createWorkflow({ id: 'two-gates-child', inputSchema: num, outputSchema: z.any() })
      .parallel([gate('left'), gate('right')])
      .commit();
    const parent = createWorkflow({ id: 'nested-parent', inputSchema: num, outputSchema: z.any() })
      .then(child)
      .commit();
    const mastra = h.mastraWith({ parent });
    const run = await mastra.getWorkflow('parent').createRun();
    expect((await run.start({ inputData: { value: 1 } })).status).toBe('suspended');

    await expect(run.resume({ step: 'two-gates-child', resumeData: { approved: true } })).rejects.toThrow(
      /Multiple suspended steps/,
    );
    const store = await mastra.getStorage()?.getStore('workflows');
    const snapshot = await store?.loadWorkflowSnapshot({
      workflowName: 'nested-parent',
      runId: run.runId,
    });
    expect(snapshot?.status).toBe('suspended');

    const once = await run.resume({
      step: ['two-gates-child', 'left'],
      resumeData: { approved: true },
    });
    expect(once.status).toBe('suspended');
    const done = await run.resume({
      step: ['two-gates-child', 'right'],
      resumeData: { approved: true },
    });
    expect(done.status).toBe('success');
  });

  it('rejects a late request for a suspension that was already resumed', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const gate = createStep({
      id: 'gate',
      inputSchema: num,
      outputSchema: num,
      resumeSchema: approval,
      execute: async ({ inputData, resumeData, suspend }) =>
        resumeData ? { value: inputData.value + 1 } : suspend({}),
    });
    // The same step suspends once per iteration.
    const wf = createWorkflow({ id: 'looping-gate', inputSchema: num, outputSchema: num })
      .dowhile(gate, async ({ inputData }) => inputData.value < 3)
      .commit();
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    const first = await run.start({ inputData: { value: 1 } });
    expect(first.status).toBe('suspended');
    const firstSuspendedAt = (first.steps.gate as any).suspendedAt;

    expect((await run.resume({ step: 'gate', resumeData: { approved: true } })).status).toBe('suspended');

    // A request that answers the first suspension, settled from outside Mastra.
    const outsider = new Resonate({ url: RESONATE_URL, group: `${h.group}-outsider` });
    try {
      const codec = new Codec();
      // The root creates the next resume promise shortly after a pass suspends.
      const pending = async () => {
        for (let attempt = 0; attempt < 100; attempt++) {
          for (let k = 0; k < 20; k++) {
            const id = `${run.runId}:${2 * k + 1}`;
            try {
              if ((await outsider.promises.get(id)).state === 'pending') return { id, k };
            } catch {}
          }
          await new Promise(r => setTimeout(r, 100));
        }
        throw new Error('no pending resume promise');
      };
      const stale = await pending();
      await outsider.promises.resolve(
        stale.id,
        codec.encode(
          encodeValue({
            resumeId: 'stale',
            step: 'gate',
            resumeData: { approved: true },
            suspendedAt: firstSuspendedAt,
          }),
        ),
      );
      // The pass rejects it: the run stays suspended and waits on a new promise.
      await expect
        .poll(async () => (await pending().catch(() => ({ k: -1 }))).k, { timeout: 15_000 })
        .toBe(stale.k + 1);
    } finally {
      await outsider.stop();
    }
    const store = await mastra.getStorage()?.getStore('workflows');
    const snapshot = await store?.loadWorkflowSnapshot({
      workflowName: 'looping-gate',
      runId: run.runId,
    });
    expect(snapshot?.status).toBe('suspended');

    const done = await run.resume({ step: 'gate', resumeData: { approved: true } });
    expect(done.status).toBe('success');
    if (done.status === 'success') expect(done.result).toEqual({ value: 3 });
  });
});
