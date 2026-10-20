# @mastra/resonate

Run [Mastra](https://mastra.ai) workflows durably on any [Resonate](https://resonatehq.io) server.

Resonate is an open-source (Apache-2.0) durable execution engine built on the
[Durable Promises](https://github.com/resonatehq/durable-promise-specification) specification. This
package is a Mastra workflow execution engine for it, alongside `@mastra/inngest`.

## Usage

```ts
import { Mastra } from '@mastra/core/mastra';
import { LibSQLStore } from '@mastra/libsql';
import { init } from '@mastra/resonate';
import { Resonate } from '@resonatehq/sdk/async';
import { z } from 'zod';

// A Resonate client: this process runs workflows and serves the worker group.
const resonate = new Resonate({ url: 'http://127.0.0.1:8001', group: 'mastra' });
const { createWorkflow, createStep } = init(resonate);

const charge = createStep({
  id: 'charge',
  inputSchema: z.object({ orderId: z.string() }),
  outputSchema: z.object({ receipt: z.string() }),
  execute: async ({ inputData }) => ({ receipt: await payments.charge(inputData.orderId) }),
});

const approve = createStep({
  id: 'approve',
  inputSchema: z.object({ receipt: z.string() }),
  outputSchema: z.object({ receipt: z.string() }),
  resumeSchema: z.object({ approved: z.boolean() }),
  execute: async ({ inputData, resumeData, suspend }) =>
    resumeData?.approved ? inputData : suspend({ reason: 'needs a manager' }),
});

export const orders = createWorkflow({
  id: 'orders',
  inputSchema: z.object({ orderId: z.string() }),
  outputSchema: z.object({ receipt: z.string() }),
  retryConfig: { attempts: 3, delay: 1_000 },
})
  .then(charge)
  .sleep(60_000)
  .then(approve)
  .commit();

// Snapshots, run listings and Studio still use Mastra storage.
export const mastra = new Mastra({
  workflows: { orders },
  storage: new LibSQLStore({ id: 'store', url: 'file:./mastra.db' }),
});

const run = await mastra.getWorkflow('orders').createRun();
const first = await run.start({ inputData: { orderId: 'o-42' } }); // 'suspended'
const done = await run.resume({ step: 'approve', resumeData: { approved: true } });
```

Adding a workflow to `Mastra` registers it as a Resonate function named by its id. Any
process that does the same, with a Resonate client in the same group, is a worker: if
the process running a workflow dies, the server hands its task to another worker, which
replays the run and continues after the last completed step. No HTTP route is needed;
workers hold a connection to the server.

`init(resonate, options)`:

| Option    | Default            | Meaning                                                                                                        |
| --------- | ------------------ | -------------------------------------------------------------------------------------------------------------- |
| `timeout` | 30 days            | Deadline of a run's durable promises. Every sleep, sleep-until and suspension must end before it.              |
| `group`   | the client's group | The worker group passes are dispatched to. Must match the client's `group`; read from the client when omitted. |

Workflow streaming and `watch()` go through the Mastra instance's `pubsub`. The default
in-process `EventEmitterPubSub` works when the caller and the worker are the same process;
for watching runs that execute on other workers, configure a shared PubSub on `Mastra`.

## How Mastra workflows map onto Resonate

`ResonateExecutionEngine` extends Mastra's `DefaultExecutionEngine` and overrides the hooks
Mastra provides for durable backends, the way `@mastra/inngest` does.

| Mastra                                                                                   | Resonate                                                                                                                                                                                                                                  |
| ---------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| A run                                                                                    | A root durable promise, id = the run id (`:` and `%` percent-encoded), created by `resonate.run(runId, workflowId, payload)`.                                                                                                             |
| One execution of the graph (start, or a resume)                                          | A pass: a durable remote call (`ctx.rpc`) to the same function, at `<root>:2k`. Each pass is its own task, recoverable on any worker in the group, and a global promise the caller waits on.                                              |
| A step, agent, tool, classifier or mapping execution                                     | A durable child (`ctx.run`) holding the whole `StepExecutionResult`. Each concurrent branch (parallel, conditional, foreach) gets its own child context, so branches never interleave durable operations.                                 |
| Each step attempt                                                                        | A durable child inside the step's context. Resonate never retries it: Mastra's `retryConfig` (or `step.retries`) decides, and the delay between attempts is a durable `ctx.sleep`. A failed attempt is recorded and not re-run on replay. |
| `wrapDurableOperation` (snapshot writes, events, condition evaluation, spans of the run) | A durable child, memoised.                                                                                                                                                                                                                |
| `.sleep()`, `.sleepUntil()`                                                              | `ctx.sleep` (a server-side timer). The task is released while it sleeps.                                                                                                                                                                  |
| `suspend()`                                                                              | The pass ends with status `suspended` (the snapshot keeps `suspendedPaths` and `resumeLabels`, as the default engine's). The root then waits on a latent durable promise at `<root>:2k+1` (`ctx.promise`).                                |
| `run.resume()`                                                                           | Settles that promise with a resume request. It settles once: a concurrent resume of the same suspension is refused. Any Resonate client may settle it, so a run can be resumed from outside Mastra.                                       |
| A nested workflow                                                                        | A durable child of the step's context, under the parent's run id with its own snapshot (what `step.invoke` does in Inngest). Nested suspend and resume work.                                                                              |
| `run.cancel()`                                                                           | Cancels the pending pass, the pending resume promise and the root. A sleeping run never wakes.                                                                                                                                            |
| `onStart`                                                                                | A pre-flight gate in `run.start()`, once per run, as in the default engine.                                                                                                                                                               |
| `onFinish`, `onError`, the run's span                                                    | Run in a memoised finalize operation, once per pass.                                                                                                                                                                                      |
| Snapshots, `listWorkflowRuns`, `getWorkflowRunById`                                      | Mastra's `workflows` storage domain, unchanged. Resonate memoises execution; it is not a queryable run store.                                                                                                                             |
| Stream and `watch()` events                                                              | Mastra's `PubSub`, with the default engine's event shapes. Events published inside a memoised operation are not re-published on replay.                                                                                                   |

### Determinism

Resonate names a durable promise by its position under its parent (`root:3.1`), so a
replay must create operations in the same order. Mastra runs concurrent branches with
`Promise.all` and a foreach queue, so the engine gives every step execution its own child
context, created synchronously when the branch starts. Operations inside a branch then
run in program order. Each memoised value carries its Mastra operation id; if a replay
asks for a different operation at a recorded position (for example because the workflow
changed while a run was in flight), the engine fails the run with `NondeterminismError`
instead of returning another operation's value.

Do not change a workflow's graph while runs of it are in flight; finish or cancel them first.

### Values and errors

Everything stored in a durable promise goes through a JSON codec that keeps errors whole
(`name`, `message`, `stack`, `cause` chain and custom properties such as `statusCode`) and
refuses functions, symbols, bigints and cycles with a `CodecError` instead of dropping
them. Dates round-trip as `Date`s. Step inputs and outputs must otherwise be
JSON-serialisable, as with any durable engine.

## Parity with `@mastra/inngest`

| Feature                                                              | `@mastra/inngest`                             | This engine                                                   |
| -------------------------------------------------------------------- | --------------------------------------------- | ------------------------------------------------------------- |
| start, `startAsync`, result                                          | yes                                           | yes                                                           |
| Step memoisation and crash recovery                                  | yes                                           | yes                                                           |
| `sleep`, `sleepUntil`                                                | yes                                           | yes                                                           |
| Retries governed by Mastra's `retryConfig`                           | yes                                           | yes                                                           |
| parallel, branch, loops, foreach                                     | yes                                           | yes                                                           |
| Nested workflows                                                     | yes (`step.invoke`)                           | yes (durable child)                                           |
| Suspend and resume, labels                                           | yes                                           | yes                                                           |
| Resume from outside Mastra                                           | by sending the resume event                   | by settling the run's resume promise from any Resonate client |
| A second, concurrent resume of one suspension                        | not checked                                   | refused                                                       |
| Nested resume                                                        | skipped in Mastra's suite (`resumeNested`)    | yes                                                           |
| `cancel`                                                             | yes (`cancelOn`)                              | yes (cancels the durable promises)                            |
| `stream`, `watch`                                                    | yes (Inngest realtime)                        | yes (Mastra PubSub)                                           |
| `onStart`, `onFinish`, `onError`                                     | `onFinish` and `onError` in its finalize step | all three                                                     |
| Workflow span                                                        | memoised                                      | memoised                                                      |
| Time travel                                                          | yes                                           | not yet                                                       |
| `restart`                                                            | no                                            | not yet                                                       |
| Flow control (concurrency, rate limit, throttle, debounce, priority) | yes                                           | no (Resonate has no equivalent)                               |
| Cron schedules                                                       | yes                                           | not yet (Resonate has schedules)                              |
| Durable agents (`createInngestAgent`)                                | yes                                           | not yet                                                       |
| Serving                                                              | an HTTP route (`serve`)                       | none: workers poll                                            |

## Limits

- Time travel and `restart` throw "not supported yet".
- `@internal/workflow-test-utils` runs in `src/index.test.ts`. Its skips are resuming one
  foreach iteration by index or label, resuming through a workflow rebuilt without Mastra,
  `restart()`, and suspend and resume through `streamLegacy`.
- Control-flow container spans (parallel, conditional, loop, foreach) are created outside
  memoised operations and can be exported again when a pass replays.
- A run's promises share one deadline (`timeout`). A resume promise that times out leaves
  the run suspended and no longer resumable through `run.resume()`.
- Watching a run that executes on another process needs a shared PubSub.
- Tested with `@resonatehq/sdk` 0.11.5 (protocol `2026-04-01`) and the Resonate server v0.9.8.

## Tests

```sh
pnpm --filter @mastra/resonate test   # starts the Resonate server with docker compose
```
