import { stepCountIs } from '@internal/ai-sdk-v5';
import { it, expect } from 'vitest';
import { z } from 'zod/v4';
import { MockMemory } from '../../../../memory';
import { createTool } from '../../../../tools';
import {
  createSharedAgent,
  describeForAllEngines,
  runApprovalScenario,
  runLoopScenario,
  useLoopScenarioAimock,
  withScenarioEventSendFailure,
} from '../aimock-scenario';
import { registerApprovalLifecycleDiagnostics } from '../approval-lifecycle-diagnostics';

/**
 * Regression class: tool approval + suspend/resume.
 *
 * With `requireToolApproval: true` the loop suspends before executing a tool,
 * emits a `tool-call-approval` chunk, and persists a snapshot. The run is then
 * resumed via approve/decline. These scenarios prove the suspend → resume →
 * continue path keeps message ordering and tool plumbing intact across the
 * snapshot boundary.
 */
describeForAllEngines('AIMock loop scenario: tool approval suspend/resume', engine => {
  const getMock = useLoopScenarioAimock();

  const makeLookupTool = () =>
    createTool({
      id: 'lookup_status',
      description: 'Look up a status payload for a query.',
      inputSchema: z.object({ query: z.string() }),
      outputSchema: z.object({ status: z.string() }),
      execute: async ({ query }) => ({ status: `STATUS_OK:${query}` }),
    });

  it('approves a suspended tool call, executes it, then completes', async () => {
    const { output, chunks, approvals, requests } = await runApprovalScenario({
      llm: getMock(),
      engine,
      prompt: 'Look up the status for query alpha.',
      tools: { lookup_status: makeLookupTool() },
      stopWhen: stepCountIs(5),
      decision: () => true,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_lookup_alpha', name: 'lookup_status', arguments: { query: 'alpha' } }] },
        );
        llm.on(
          { endpoint: 'chat', toolCallId: 'call_lookup_alpha', hasToolResult: true },
          { content: 'The status for alpha is STATUS_OK:alpha.' },
        );
      },
    });

    // The loop suspended for exactly one approval, which we approved.
    expect(approvals).toEqual(['approve:call_lookup_alpha']);

    // The approval chunk was surfaced before resume.
    expect(chunks.some(chunk => chunk.type === 'tool-call-approval')).toBe(true);

    // After approval the tool executed and the model produced its final answer.
    const text = await output.text;
    expect(text).toContain('STATUS_OK:alpha');

    // Two model turns total: the tool-call turn and the post-result turn. The
    // approval pause does not insert an extra model request.
    expect(requests).toHaveLength(2);

    // The post-resume request carries the executed tool result keyed to the
    // original tool call id.
    const turn2Messages = requests[1]?.body?.messages ?? [];
    const toolMessage = turn2Messages.find(message => (message as { role?: string }).role === 'tool') as
      | { tool_call_id?: string }
      | undefined;
    expect(toolMessage?.tool_call_id).toBe('call_lookup_alpha');
  });

  it('declines a suspended tool call and reports the denial back to the model', async () => {
    const { output, approvals, requests } = await runApprovalScenario({
      llm: getMock(),
      engine,
      prompt: 'Look up the status for query beta.',
      tools: { lookup_status: makeLookupTool() },
      stopWhen: stepCountIs(5),
      decision: () => false,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_lookup_beta', name: 'lookup_status', arguments: { query: 'beta' } }] },
        );
        // After the decline is reported as the tool result, the model wraps up.
        llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Understood, I will not look that up.' });
      },
    });

    expect(approvals).toEqual(['decline:call_lookup_beta']);

    // A declined tool must NOT produce its real execution payload downstream.
    const turn2Messages = requests[1]?.body?.messages ?? [];
    const serialized = JSON.stringify(turn2Messages);
    expect(serialized).not.toContain('STATUS_OK:beta');

    const text = await output.text;
    expect(text).toContain('will not look that up');
  });

  it('declineToolCall never executes when gated only by function requireToolApproval (#20470)', async () => {
    let executions = 0;
    const dangerTool = createTool({
      id: 'do_the_thing',
      description: 'Performs an irreversible action',
      inputSchema: z.object({ label: z.string() }),
      outputSchema: z.object({ done: z.boolean() }),
      execute: async () => {
        executions += 1;
        return { done: true };
      },
    });

    const { approvals, requests } = await runApprovalScenario({
      llm: getMock(),
      engine,
      prompt: 'Use the label "beta".',
      tools: { do_the_thing: dangerTool },
      stopWhen: stepCountIs(5),
      requireToolApproval: ({ toolName }) => toolName === 'do_the_thing',
      decision: () => false,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_20470_stream', name: 'do_the_thing', arguments: { label: 'beta' } }] },
        );
        llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Declined.' });
      },
    });

    expect(approvals).toEqual(['decline:call_20470_stream']);
    expect(executions).toBe(0);
    expect(JSON.stringify(requests[1]?.body?.messages ?? [])).not.toContain('"done":true');
  });

  // A user clicking Approve twice must not run the tool twice, and the second
  // click must fail right away instead of waiting on the first run (#27365).
  it('approving the same tool call twice runs it once and fails the second approval fast', async () => {
    let executions = 0;
    let release!: () => void;
    const gate = new Promise<void>(resolve => {
      release = resolve;
    });
    const archiveTool = createTool({
      id: 'archive',
      description: 'Archive an item',
      inputSchema: z.object({ item: z.string() }),
      requireApproval: true,
      execute: async () => {
        executions += 1;
        await gate;
        return { archived: true };
      },
    });

    const llm = getMock();
    const memory = new MockMemory();
    const shared = await createSharedAgent(llm, { tools: { archive: archiveTool }, memory, engine });
    const { output, chunks } = await runLoopScenario({
      engine,
      llm,
      sharedAgent: shared,
      prompt: 'Archive a',
      memory,
      threadId: `double-approve-thread-${engine}-${crypto.randomUUID()}`,
      resourceId: 'double-approve-resource',
      collectChunks: true,
      fixtures: mock => {
        mock.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_archive', name: 'archive', arguments: { item: 'a' } }] },
        );
        mock.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Archived.' });
      },
    });
    expect(chunks!.some(chunk => chunk.type === 'tool-call-approval')).toBe(true);

    let firstResult: any;
    let secondResult: any;
    let secondOutcome: any;
    const resumedTypes: string[][] = [];
    try {
      const first = shared.agent.approveToolCall({ runId: output.runId, toolCallId: 'call_archive' });
      const startedAt = performance.now();
      const second = shared.agent.approveToolCall({ runId: output.runId, toolCallId: 'call_archive' });
      const settledSecond = second.then(
        value => ({ status: 'resolved', value, elapsedMs: performance.now() - startedAt }),
        error => ({
          status: 'rejected',
          error: { id: error.id, message: error.message },
          elapsedMs: performance.now() - startedAt,
        }),
      );
      let timer: ReturnType<typeof setTimeout>;
      secondOutcome = await Promise.race([
        settledSecond,
        new Promise(resolve => {
          timer = setTimeout(() => resolve({ status: 'pending', elapsedMs: performance.now() - startedAt }), 1_000);
        }),
      ]);
      clearTimeout(timer!);
      expect.soft(secondOutcome.status, 'second approval rejects promptly').toBe('rejected');
      expect.soft(secondOutcome.error?.id, 'duplicate-approval error code').toBe('AGENT_RESUME_ALREADY_CLAIMED');
      expect.soft(secondOutcome.elapsedMs, 'duplicate rejection deadline').toBeLessThan(1_000);
      release();
      firstResult = await first;
      const drain = async (result: any) => {
        const types: string[] = [];
        for await (const chunk of result.fullStream) {
          types.push(chunk.type);
        }
        resumedTypes.push(types);
      };
      const eventual = await settledSecond;
      if (eventual.status === 'resolved') secondResult = (eventual as any).value;
      await Promise.all([drain(firstResult), ...(secondResult ? [drain(secondResult)] : [])]);
      expect.soft(resumedTypes[0], 'first approval completes').toContain('finish');
      expect.soft(resumedTypes.flat(), 'resumed streams have no errors').not.toContain('error');
      expect.soft(executions, 'tool executes once').toBe(1);
    } finally {
      release();
      firstResult?.cleanup?.();
      secondResult?.cleanup?.();
    }
  });

  it('requireToolApproval in agent defaultOptions gates tool calls', async () => {
    let executions = 0;
    const archiveTool = createTool({
      id: 'archive',
      description: 'Archive an item',
      inputSchema: z.object({ item: z.string() }),
      execute: async () => {
        executions += 1;
        return { archived: true };
      },
    });

    const llm = getMock();
    const memory = new MockMemory();
    const shared = await createSharedAgent(llm, {
      tools: { archive: archiveTool },
      memory,
      engine,
      defaultOptions: { requireToolApproval: true },
    });
    const { output, chunks } = await runLoopScenario({
      engine,
      llm,
      sharedAgent: shared,
      prompt: 'Archive a',
      memory,
      threadId: 'default-options-approval-thread',
      resourceId: 'default-options-approval-resource',
      collectChunks: true,
      fixtures: mock => {
        mock.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: 'call_archive', name: 'archive', arguments: { item: 'a' } }] },
        );
        mock.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Archived.' });
      },
    });
    expect(chunks!.some(chunk => chunk.type === 'tool-call-approval')).toBe(true);
    expect(executions).toBe(0);

    const resumed = await shared.agent.approveToolCall({ runId: output.runId, toolCallId: 'call_archive' });
    for await (const _ of resumed.fullStream) {
      // drain
    }
    expect(executions).toBe(1);
    expect(await resumed.text).toBe('Archived.');
  });
  const startClaimScenario = async (tool: ReturnType<typeof createTool>, callId: string) => {
    const llm = getMock();
    const memory = new MockMemory();
    const shared = await createSharedAgent(llm, { tools: { operate: tool }, memory, engine });
    const { output } = await runLoopScenario({
      engine,
      llm,
      sharedAgent: shared,
      prompt: 'Operate on item a.',
      memory,
      threadId: `claim-${engine}-${callId}-${crypto.randomUUID()}`,
      resourceId: 'claim-reviewer',
      collectChunks: true,
      fixtures: mock => {
        mock.on(
          { endpoint: 'chat', hasToolResult: false },
          { toolCalls: [{ id: callId, name: 'operate', arguments: { item: 'a' } }] },
        );
        mock.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Done.' });
      },
    });
    return { shared, runId: output.runId };
  };

  const readApprovalSegment = async (output: any) => {
    const chunks: any[] = [];
    for await (const chunk of output.fullStream) chunks.push(chunk);
    return chunks;
  };

  it('a failed resume setup leaves the same approval usable (#27485)', async () => {
    let executions = 0;
    const tool = createTool({
      id: 'operate',
      description: 'Operate after approval.',
      inputSchema: z.object({ item: z.string() }),
      requireApproval: true,
      execute: async () => {
        executions++;
        return { done: true };
      },
    });
    const { shared, runId } = await startClaimScenario(tool, 'call_setup_failure');
    await expect(
      shared.agent.resumeStream(
        { approved: true },
        { runId, toolCallId: 'call_setup_failure', modelSettings: { timeout: { totalMs: -1 } } },
      ),
    ).rejects.toThrow(/timeout/i);
    const result = await shared.agent.approveToolCall({ runId, toolCallId: 'call_setup_failure' });
    const chunks = await readApprovalSegment(result);
    expect(chunks.some(chunk => chunk.type === 'finish')).toBe(true);
    expect(chunks.filter(chunk => chunk.type === 'error')).toEqual([]);
    expect(executions).toBe(1);
  });

  it('the same tool can suspend again and accept a later approval (#27485)', async () => {
    const stages: number[] = [];
    const tool = createTool({
      id: 'operate',
      description: 'Complete two separately approved stages.',
      inputSchema: z.object({ item: z.string() }),
      requireApproval: true,
      execute: async (_input, context) => {
        const stage = (context?.agent?.resumeData as { stage?: number } | undefined)?.stage;
        stages.push(stage!);
        if (stage === 1) return context?.agent?.suspend({ nextStage: 2 });
        return { done: true };
      },
    });
    const { shared, runId } = await startClaimScenario(tool, 'call_two_stages');
    const first = await shared.agent.resumeStream(
      { approved: true, stage: 1 },
      { runId, toolCallId: 'call_two_stages', closeOnSuspend: true },
    );
    const firstChunks = await readApprovalSegment(first);
    expect(firstChunks.some(chunk => chunk.type === 'tool-call-suspended')).toBe(true);
    const second = await shared.agent.resumeStream(
      { approved: true, stage: 2 },
      { runId, toolCallId: 'call_two_stages', closeOnSuspend: true },
    );
    const secondChunks = await readApprovalSegment(second);
    expect(secondChunks.some(chunk => chunk.type === 'finish')).toBe(true);
    expect(secondChunks.filter(chunk => chunk.type === 'error')).toEqual([]);
    expect(stages).toEqual([1, 2]);
  });

  const itUsesInngestTransport = engine === 'inngest' ? it : it.skip;
  itUsesInngestTransport('a failed HTTP resume dispatch releases its claim (#27485)', async () => {
    let executions = 0;
    const tool = createTool({
      id: 'operate',
      description: 'Operate after approval.',
      inputSchema: z.object({ item: z.string() }),
      requireApproval: true,
      execute: async () => {
        executions++;
        return { done: true };
      },
    });
    const { shared, runId } = await startClaimScenario(tool, 'call_dispatch_failure');
    await expect(
      withScenarioEventSendFailure(() =>
        shared.agent.resumeStream({ approved: true }, { runId, toolCallId: 'call_dispatch_failure' }),
      ),
    ).rejects.toThrow(/fixture dispatch unavailable/);
    const result = await shared.agent.approveToolCall({ runId, toolCallId: 'call_dispatch_failure' });
    const chunks = await readApprovalSegment(result);
    expect(chunks.some(chunk => chunk.type === 'finish')).toBe(true);
    expect(chunks.filter(chunk => chunk.type === 'error')).toEqual([]);
    expect(executions).toBe(1);
  });
});

registerApprovalLifecycleDiagnostics();
