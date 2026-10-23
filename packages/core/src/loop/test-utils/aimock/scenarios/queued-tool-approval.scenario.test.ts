import { expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { agentThreadStreamRuntime } from '../../../../agent/durable';
import { DurableStepIds } from '../../../../agent/durable/constants';
import { MockMemory } from '../../../../memory/mock';
import { createTool } from '../../../../tools';
import {
  createSharedAgent,
  describeForAllEngines,
  isDurableEngineVariant,
  runLoopScenario,
  useLoopScenarioAimock,
} from '../aimock-scenario';

// #25831: approving one of two suspended calls must not strand the queued approval
// when the first resume re-suspends. Exercise the public thread approval queue.
describeForAllEngines(
  'AIMock loop scenario: queued approvals survive re-suspension',
  engine => {
    const getMock = useLoopScenarioAimock();
    // Diagnostic branch: expose #27925, including duplicated actions and an unapproved sibling.
    const approvalTest = it;

    for (let repetition = 0; repetition < Number(process.env.MASTRA_APPROVAL_REPETITIONS ?? 1); repetition++) {
      approvalTest.each(['forward', 'reverse', 'concurrent'] as const)(
        `finishes both calls approved in %s order (repeat ${repetition + 1})`,
        async order => {
          const llm = getMock();
          llm.on(
            { endpoint: 'chat', hasToolResult: false },
            {
              toolCalls: [
                { id: 'call-a', name: 'confirm', arguments: { item: 'A' } },
                { id: 'call-b', name: 'confirm', arguments: { item: 'B' } },
              ],
            },
          );
          llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Both done.' });
          const committed: string[] = [];
          const confirm = createTool({
            id: 'confirm',
            description: 'Confirm an item before committing it.',
            inputSchema: z.object({ item: z.string() }),
            execute: async ({ item }, context) => {
              if (!context?.agent?.resumeData) return context?.agent?.suspend({ item });
              committed.push(item);
              return { item, confirmed: true };
            },
          });
          const { agent, mastra } = await createSharedAgent(llm, {
            engine,
            memory: new MockMemory(),
            tools: { confirm },
          });
          const threadId = `queued-${engine}-${order}-${crypto.randomUUID()}`;
          const resourceId = 'reviewer';
          const chunks: any[] = [];
          const subscription = await agent.subscribeToThread({ threadId, resourceId });
          const drain = (async () => {
            for await (const chunk of subscription.stream) {
              chunks.push(chunk);
            }
          })();
          let initial: { runId: string; cleanup?: () => void } | undefined;
          try {
            initial = await agent.stream('Confirm both items.', {
              maxSteps: 4,
              memory: { thread: threadId, resource: resourceId },
            });
            await vi.waitFor(() => expect(chunks.filter(c => c.type === 'tool-call-suspended')).toHaveLength(2), {
              timeout: 10_000,
            });
            if (isDurableEngineVariant(engine) && engine !== 'inngest') {
              const workflows = await mastra.getStorage().getStore('workflows');
              await vi.waitFor(
                async () => {
                  const persisted = await workflows.getWorkflowRunById({
                    workflowName: DurableStepIds.AGENTIC_LOOP,
                    runId: initial!.runId,
                  });
                  const snapshot =
                    typeof persisted?.snapshot === 'string' ? JSON.parse(persisted.snapshot) : persisted?.snapshot;
                  expect(snapshot?.status).toBe('suspended');
                },
                { timeout: 10_000 },
              );
            }
            const didPublishTerminal =
              engine === 'inngest'
                ? agentThreadStreamRuntime.captureThreadRunTerminalPublish(initial!.runId, agent.getPubSub())
                : undefined;
            if (engine === 'inngest') expect(didPublishTerminal).toBeTypeOf('function');
            const ids = order === 'reverse' ? ['call-b', 'call-a'] : ['call-a', 'call-b'];
            const approve = (toolCallId: string) =>
              agent.sendToolApproval({
                threadId,
                resourceId,
                toolCallId,
                approved: true,
                resumeData: { confirmed: true },
              });
            const decisions =
              order === 'concurrent'
                ? await Promise.all(ids.map(approve))
                : [await approve(ids[0]!), await approve(ids[1]!)];
            expect(decisions.map(d => d.toolCallId)).toEqual(ids);
            await vi.waitFor(
              () => expect(chunks.some(c => c.type === 'finish' && c.payload.stepResult?.reason === 'stop')).toBe(true),
              { timeout: 10_000 },
            );
            if (didPublishTerminal)
              await vi.waitFor(() => expect(didPublishTerminal()).toBe(true), { timeout: 10_000 });
            await mastra.pubsub?.flush();
            await (agent as any).pubsub?.flush();
            subscription.unsubscribe();
            await drain;
            expect
              .soft(
                chunks
                  .filter(c => c.type === 'text-delta')
                  .map(c => c.payload.text)
                  .join(''),
                'one final answer',
              )
              .toBe('Both done.');
            expect.soft(llm.getRequests(), 'one initial and one final model request').toHaveLength(2);
            expect.soft(committed.sort(), 'each tool executes once').toEqual(['A', 'B']);
            expect
              .soft(
                chunks
                  .filter(c => c.type === 'tool-result')
                  .map(c => c.payload.toolCallId)
                  .sort(),
              )
              .toEqual(['call-a', 'call-b']);
            expect.soft(chunks.filter(c => c.type === 'tool-error' || c.type === 'error')).toEqual([]);
            const lastRequest = llm.getRequests().at(-1)!;
            expect
              .soft(
                lastRequest.body.messages
                  .filter((m: any) => m.role === 'tool')
                  .map((m: any) => m.tool_call_id)
                  .sort(),
              )
              .toEqual(['call-a', 'call-b']);
          } finally {
            initial?.cleanup?.();
            subscription.unsubscribe();
            await drain;
            if (engine !== 'inngest') await mastra.shutdown({ drainTimeout: 1000 });
          }
        },
        30_000,
      );
    }
    it.each(['call-a', 'call-b'])('approving %s leaves its sibling suspended (#27925)', async firstId => {
      const llm = getMock();
      const committed: string[] = [];
      const confirm = createTool({
        id: 'confirm',
        description: 'Confirm an item before committing it.',
        inputSchema: z.object({ item: z.string() }),
        execute: async ({ item }, context) => {
          if (!context?.agent?.resumeData) return context?.agent?.suspend({ item });
          committed.push(item);
          return { item, confirmed: true };
        },
      });
      const memory = new MockMemory();
      const shared = await createSharedAgent(llm, { engine, memory, tools: { confirm } });
      const { output } = await runLoopScenario({
        engine,
        llm,
        sharedAgent: shared,
        memory,
        prompt: 'Confirm both items.',
        threadId: `selected-${engine}-${firstId}-${crypto.randomUUID()}`,
        resourceId: 'selected-reviewer',
        collectChunks: true,
        fixtures: mock => {
          mock.on(
            { endpoint: 'chat', hasToolResult: false },
            {
              toolCalls: [
                { id: 'call-a', name: 'confirm', arguments: { item: 'A' } },
                { id: 'call-b', name: 'confirm', arguments: { item: 'B' } },
              ],
            },
          );
          mock.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Both done.' });
        },
      });
      const first = await shared.agent.approveToolCall({
        runId: output.runId,
        toolCallId: firstId,
        closeOnSuspend: true,
      });
      for await (const _chunk of first.fullStream) {
        /* settle the first approved segment */
      }
      expect
        .soft(committed, 'only the selected tool has permission to execute')
        .toEqual([firstId === 'call-a' ? 'A' : 'B']);
      const secondId = firstId === 'call-a' ? 'call-b' : 'call-a';
      const second = await shared.agent.approveToolCall({
        runId: output.runId,
        toolCallId: secondId,
        closeOnSuspend: true,
      });
      const chunks: any[] = [];
      for await (const chunk of second.fullStream) chunks.push(chunk);
      expect(committed.sort()).toEqual(['A', 'B']);
      expect(
        chunks
          .filter(chunk => chunk.type === 'text-delta')
          .map(chunk => chunk.payload.text)
          .join(''),
      ).toBe('Both done.');
      expect(llm.getRequests()).toHaveLength(2);
      if (engine !== 'inngest') await shared.mastra.shutdown({ drainTimeout: 1000 });
    });
  },
  { skip: ['fs'] },
);
