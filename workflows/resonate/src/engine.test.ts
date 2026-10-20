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

function counter() {
  const counts: Record<string, number> = {};
  return {
    counts,
    hit(id: string) {
      counts[id] = (counts[id] ?? 0) + 1;
    },
  };
}

/** Records the durable promises created on the server: id relative to the root, and the Mastra operation tag. */
function recordCreates() {
  const original = globalThis.fetch;
  const created: { id: string; op?: string }[] = [];
  globalThis.fetch = async (url: any, init?: any) => {
    try {
      const body = typeof init?.body === 'string' ? JSON.parse(init.body) : undefined;
      const action = body?.kind === 'task.fence' ? body.data?.action : body;
      if (action?.kind === 'promise.create') {
        created.push({ id: action.data.id, op: action.data.tags?.['mastra:op'] });
      }
    } catch {}
    return original(url, init);
  };
  return {
    created,
    restore() {
      globalThis.fetch = original;
    },
  };
}

describe.skipIf(!hasServer)('ResonateExecutionEngine', () => {
  it('runs_to_completion', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const add = (id: string, n: number) =>
      createStep({
        id,
        inputSchema: num,
        outputSchema: num,
        execute: async ({ inputData }) => ({ value: inputData.value + n }),
      });
    const wf = createWorkflow({ id: 'three-steps', inputSchema: num, outputSchema: num })
      .then(add('one', 1))
      .then(add('two', 10))
      .then(add('three', 100))
      .commit();
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    const result = await run.start({ inputData: { value: 0 } });
    expect(result.status).toBe('success');
    if (result.status === 'success') expect(result.result).toEqual({ value: 111 });
    expect(result.steps.two).toMatchObject({ status: 'success', output: { value: 11 } });
    // start() returns when the pass is done; the root promise settles right after.
    await expect.poll(() => (run as any).durableStatus(), { timeout: 10_000 }).toBe('success');
  });

  it('steps_are_memoised_on_replay', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const c = counter();
    const step = (id: string) =>
      createStep({
        id,
        inputSchema: num,
        outputSchema: num,
        execute: async ({ inputData }) => {
          c.hit(id);
          return { value: inputData.value + 1 };
        },
      });
    // The sleep ends the first pass of the run's task; the task then replays from
    // the top, and the steps before the sleep must come from their promises.
    const wf = createWorkflow({ id: 'memo', inputSchema: num, outputSchema: num })
      .then(step('before'))
      .sleep(300)
      .then(step('after'))
      .commit();
    const mastra = h.mastraWith({ wf });
    const result = await (
      await mastra.getWorkflow('wf').createRun()
    ).start({
      inputData: { value: 1 },
    });
    expect(result.status).toBe('success');
    if (result.status === 'success') expect(result.result).toEqual({ value: 3 });
    expect(c.counts).toEqual({ before: 1, after: 1 });
  });

  it('sleep_and_sleep_until_are_durable', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const c = counter();
    const mark = (id: string) =>
      createStep({
        id,
        inputSchema: num,
        outputSchema: num,
        execute: async ({ inputData }) => {
          c.hit(id);
          return inputData;
        },
      });
    const wf = createWorkflow({ id: 'sleepy', inputSchema: num, outputSchema: num })
      .then(mark('mark'))
      .sleep(800)
      .sleepUntil(async () => new Date(Date.now() + 800))
      .then(mark('end'))
      .commit();
    const mastra = h.mastraWith({ wf });
    const recorder = recordCreates();
    const started = Date.now();
    const run = await mastra.getWorkflow('wf').createRun();
    let result: Awaited<ReturnType<typeof run.start>>;
    try {
      result = await run.start({ inputData: { value: 7 } });
    } finally {
      recorder.restore();
    }
    expect(result.status).toBe('success');
    expect(Date.now() - started).toBeGreaterThanOrEqual(1500);
    expect(c.counts).toEqual({ mark: 1, end: 1 });
    // Both sleeps are server-side timers (durable promises), not in-process waits.
    const timers = recorder.created.filter(p => p.op === undefined && /:\d+(\.\d+)+$/.test(p.id));
    expect(timers.length).toBeGreaterThanOrEqual(2);
  });

  it('nested_workflow_runs_as_child', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const double = createStep({
      id: 'double',
      inputSchema: num,
      outputSchema: num,
      execute: async ({ inputData }) => ({ value: inputData.value * 2 }),
    });
    const inc = createStep({
      id: 'inc',
      inputSchema: num,
      outputSchema: num,
      execute: async ({ inputData }) => ({ value: inputData.value + 1 }),
    });
    const child = createWorkflow({ id: 'child', inputSchema: num, outputSchema: num })
      .then(double)
      .sleep(200)
      .then(double)
      .commit();
    const parent = createWorkflow({ id: 'parent', inputSchema: num, outputSchema: num })
      .then(inc)
      .then(child)
      .then(inc)
      .commit();
    const mastra = h.mastraWith({ parent });
    const run = await mastra.getWorkflow('parent').createRun();
    const result = await run.start({ inputData: { value: 1 } });
    expect(result.status).toBe('success');
    if (result.status === 'success') expect(result.result).toEqual({ value: 9 });
    expect(result.steps.child).toMatchObject({ status: 'success', output: { value: 8 } });
    const store = await mastra.getStorage()!.getStore('workflows');
    const nested = await store!.loadWorkflowSnapshot({ workflowName: 'child', runId: run.runId });
    expect(nested?.status).toBe('success');
  });

  it('retries_follow_mastra_retry_config_only', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const c = counter();
    const flaky = createStep({
      id: 'flaky',
      inputSchema: num,
      outputSchema: num,
      execute: async ({ inputData }) => {
        c.hit('flaky');
        if ((c.counts.flaky ?? 0) <= 2) throw new Error(`transient ${c.counts.flaky}`);
        return inputData;
      },
    });
    const broken = createStep({
      id: 'broken',
      inputSchema: num,
      outputSchema: num,
      execute: async () => {
        c.hit('broken');
        throw Object.assign(new Error('always'), { statusCode: 503 });
      },
    });
    const ok = createWorkflow({
      id: 'retry-ok',
      inputSchema: num,
      outputSchema: num,
      retryConfig: { attempts: 3, delay: 150 },
    })
      .then(flaky)
      .commit();
    const ko = createWorkflow({
      id: 'retry-ko',
      inputSchema: num,
      outputSchema: num,
      retryConfig: { attempts: 2, delay: 0 },
    })
      .then(broken)
      .commit();
    const mastra = h.mastraWith({ ok, ko });

    const okResult = await (
      await mastra.getWorkflow('ok').createRun()
    ).start({
      inputData: { value: 1 },
    });
    expect(okResult.status).toBe('success');
    expect(c.counts.flaky).toBe(3);

    const koResult = await (
      await mastra.getWorkflow('ko').createRun()
    ).start({
      inputData: { value: 1 },
    });
    expect(koResult.status).toBe('failed');
    // attempts: 2 means two retries, three runs in total, never more.
    expect(c.counts.broken).toBe(3);
    if (koResult.status === 'failed') {
      expect(koResult.error).toBeInstanceOf(Error);
      expect((koResult.steps.broken as any).error.statusCode).toBe(503);
    }
  });

  it('operation_ids_are_deterministic', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const step = (id: string) =>
      createStep({
        id,
        inputSchema: num,
        outputSchema: num,
        execute: async ({ inputData }) => ({ value: inputData.value + 1 }),
      });
    const branchA = step('a');
    const branchB = step('b');
    const wf = createWorkflow({ id: 'ids', inputSchema: num, outputSchema: z.any() })
      .then(step('first'))
      .parallel([branchA, branchB])
      .map(async ({ inputData }) => ({ value: inputData.a!.value + inputData.b!.value }))
      .then(step('last'))
      .commit();
    const mastra = h.mastraWith({ wf });

    const shapes: string[][] = [];
    for (let i = 0; i < 2; i++) {
      const run = await mastra.getWorkflow('wf').createRun();
      const recorder = recordCreates();
      try {
        const result = await run.start({ inputData: { value: 0 } });
        expect(result.status).toBe('success');
      } finally {
        recorder.restore();
      }
      const root = run.runId;
      shapes.push(
        recorder.created
          .filter(p => p.id.startsWith(`${root}:`))
          .map(p => `${p.id.slice(root.length)} ${(p.op ?? '').replaceAll(root, '<run>')}`)
          .sort(),
      );
    }
    expect(shapes[0]!.length).toBeGreaterThan(5);
    expect(shapes[1]).toEqual(shapes[0]);
  });

  it('on_finish_fires_once', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const c = counter();
    const step = createStep({
      id: 's',
      inputSchema: num,
      outputSchema: num,
      execute: async ({ inputData }) => inputData,
    });
    const wf = createWorkflow({
      id: 'callbacks',
      inputSchema: num,
      outputSchema: num,
      options: {
        onStart: () => c.hit('onStart'),
        onFinish: () => c.hit('onFinish'),
      },
    })
      .then(step)
      .sleep(200)
      .then(createStep({ ...step, id: 's2' } as any))
      .commit();
    const mastra = h.mastraWith({ wf });
    const result = await (
      await mastra.getWorkflow('wf').createRun()
    ).start({
      inputData: { value: 1 },
    });
    expect(result.status).toBe('success');
    expect(c.counts).toEqual({ onStart: 1, onFinish: 1 });
  });

  it('runs parallel branches and foreach items once each across replays', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const c = counter();
    const work = (id: string) =>
      createStep({
        id,
        inputSchema: num,
        outputSchema: num,
        execute: async ({ inputData }) => {
          c.hit(`${id}:${inputData.value}`);
          await new Promise(r => setTimeout(r, Math.random() * 30));
          return { value: inputData.value * 10 };
        },
      });
    const sleeper = createWorkflow({ id: 'sleeper', inputSchema: num, outputSchema: num })
      .then(work('inner'))
      .sleep(150)
      .commit();
    const wf = createWorkflow({
      id: 'fanout',
      inputSchema: z.object({ items: z.array(num) }),
      outputSchema: z.any(),
    })
      .map(async ({ inputData }) => inputData.items)
      .foreach(work('item'), { concurrency: 3 })
      .map(async ({ inputData }) => ({ value: inputData.length }))
      .parallel([sleeper, work('side')])
      .commit();
    const mastra = h.mastraWith({ wf });
    const items = [1, 2, 3, 4, 5, 6].map(value => ({ value }));
    const result = await (
      await mastra.getWorkflow('wf').createRun()
    ).start({
      inputData: { items },
    });
    expect(result.status).toBe('success');
    if (result.status === 'success') expect(result.result).toEqual({ sleeper: { value: 60 }, side: { value: 60 } });
    for (const { value } of items) expect(c.counts[`item:${value}`]).toBe(1);
    expect(c.counts['inner:6']).toBe(1);
    expect(c.counts['side:6']).toBe(1);
  });

  it('cancel stops a run during a durable sleep', async () => {
    h = harness();
    const { createWorkflow, createStep } = h;
    const c = counter();
    const mark = (id: string) =>
      createStep({
        id,
        inputSchema: num,
        outputSchema: num,
        execute: async ({ inputData }) => {
          c.hit(id);
          return inputData;
        },
      });
    const wf = createWorkflow({ id: 'cancelled', inputSchema: num, outputSchema: num })
      .then(mark('before'))
      .sleep(1_500)
      .then(mark('after'))
      .commit();
    const mastra = h.mastraWith({ wf });
    const run = await mastra.getWorkflow('wf').createRun();
    await run.startAsync({ inputData: { value: 1 } });
    await expect.poll(() => c.counts.before, { timeout: 10_000 }).toBe(1);
    await run.cancel();
    await new Promise(r => setTimeout(r, 2_500));
    expect(c.counts.after).toBeUndefined();
    expect(await (run as any).durableStatus()).toBe('canceled');
    const store = await mastra.getStorage()?.getStore('workflows');
    const snapshot = await store?.loadWorkflowSnapshot({
      workflowName: 'cancelled',
      runId: run.runId,
    });
    expect(snapshot?.status).toBe('canceled');
  });

  it('a sleep longer than half the remaining deadline survives its replay', async () => {
    // The wake time is recorded once. Recomputing it on the replay after the timer
    // (now + duration) would put it past the deadline and fail a sleep that completed.
    h = harness({ timeout: 6_000 });
    const { createWorkflow, createStep } = h;
    const step = createStep({
      id: 'after',
      inputSchema: num,
      outputSchema: num,
      execute: async ({ inputData }) => inputData,
    });
    const wf = createWorkflow({ id: 'long-sleep', inputSchema: num, outputSchema: num })
      .sleep(3_500)
      .then(step)
      .commit();
    const mastra = h.mastraWith({ wf });
    const result = await (
      await mastra.getWorkflow('wf').createRun()
    ).start({
      inputData: { value: 1 },
    });
    expect(result.status).toBe('success');
  });
});
