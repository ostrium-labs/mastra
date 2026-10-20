---
'@mastra/resonate': minor
---

Added `@mastra/resonate`, a workflow execution engine that runs Mastra workflows on a [Resonate](https://resonatehq.io) durable execution server. Steps are memoized in durable promises, so a run that crashes continues on another worker after the last completed step. Sleeps are durable timers, suspended runs are resumed by settling a durable promise, and retries follow the workflow's `retryConfig`.

```ts
import { init } from '@mastra/resonate';
import { Resonate } from '@resonatehq/sdk/async';

const resonate = new Resonate({ url: 'http://localhost:8001', group: 'mastra' });
const { createWorkflow, createStep } = init(resonate);
```
