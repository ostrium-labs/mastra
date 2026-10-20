import { createWorkflowTestSuite } from '@internal/workflow-test-utils';
import type { ResumeWorkflowOptions, WorkflowRegistry, WorkflowResult } from '@internal/workflow-test-utils';
import { Mastra } from '@mastra/core/mastra';
import type { MastraStorage } from '@mastra/core/storage';
import { LibSQLStore } from '@mastra/libsql';
import { Resonate } from '@resonatehq/sdk/async';
import { RESONATE_URL, uniqueId } from './__tests__/helpers';
import { init } from './index';

let resonate: Resonate | undefined;
let storage: MastraStorage | undefined;

createWorkflowTestSuite({
  name: 'Workflow (Resonate Engine)',

  getWorkflowFactory: () => {
    resonate ??= new Resonate({ url: RESONATE_URL, group: uniqueId('workflow-tests'), ttl: 5_000 });
    return init(resonate);
  },

  registerWorkflows: async (registry: WorkflowRegistry) => {
    const workflows: Record<string, any> = {};
    const agents: Record<string, any> = {};
    const tools: Record<string, any> = {};
    for (const [id, entry] of Object.entries(registry)) {
      workflows[id] = entry.workflow;
      Object.assign(agents, entry.mastraAgents ?? {});
      Object.assign(tools, entry.mastraTools ?? {});
    }
    storage = new LibSQLStore({ id: 'resonate-workflow-tests', url: ':memory:' });
    new Mastra({
      storage,
      workflows,
      agents: Object.keys(agents).length ? agents : undefined,
      tools: Object.keys(tools).length ? tools : undefined,
      logger: false,
    });
  },

  getStorage: () => storage,

  afterAll: async () => {
    await resonate?.stop();
  },

  skip: {
    // restart() is not supported on the Resonate engine yet.
    restart: true,
  },

  skipTests: {
    // Resuming one foreach iteration by forEachIndex or label is not supported yet.
    resumeForeachIndex: true,
    resumeForeachLabel: true,
    resumeForeachPartialIndex: true,
    // Resumes through a rebuilt workflow that is not registered with Mastra (no storage).
    resumeMapBranchCondition: true,
    // The legacy stream does not close after suspend and resume; stream() is supported.
    streamingSuspendResumeLegacy: true,
  },

  executeWorkflow: async (workflow, inputData, options = {}): Promise<WorkflowResult> => {
    const run = await workflow.createRun({ runId: options.runId, resourceId: options.resourceId });
    const result = await run.start({
      inputData,
      initialState: options.initialState,
      perStep: options.perStep,
      requestContext: options.requestContext as any,
      outputOptions: options.outputOptions,
    });
    return result as WorkflowResult;
  },

  resumeWorkflow: async (workflow, options: ResumeWorkflowOptions): Promise<WorkflowResult> => {
    const run = await workflow.createRun({ runId: options.runId });
    const result = await run.resume({
      step: options.step,
      label: options.label,
      resumeData: options.resumeData,
    } as any);
    return result as WorkflowResult;
  },
});
