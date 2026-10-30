import { createOpenAI } from '@ai-sdk/openai-v5';
import { it, expect } from 'vitest';
import { Memory } from '../../../../../../memory/src';
import type { MastraDBMessage } from '../../../../agent/message-list';
import { MockMemory } from '../../../../memory/mock';
import type { OutputProcessor } from '../../../../processors';
import { InMemoryStore } from '../../../../storage';
import {
  createSharedAgent,
  runLoopScenario,
  useLoopScenarioAimock,
  describeForAllEngines,
  isDurableEngineVariant,
} from '../aimock-scenario';
import { SCENARIO_MODEL_ID } from '../types';

/**
 * Regression class: working-memory persistence + injection across turns.
 *
 * When memory has working memory enabled and a thread/resource is supplied, the
 * loop auto-injects the `updateWorkingMemory` tool. In turn 1 the model calls it
 * to persist a fact. The loop must (a) execute that tool so the value is stored,
 * and (b) inject the stored working memory into the system prompt on a later
 * turn. A regression in working-memory wiring (tool not injected, value not
 * persisted, or not re-injected) is caught here.
 */
describeForAllEngines('AIMock loop scenario: working memory', engine => {
  const getMock = useLoopScenarioAimock();

  it('persists working memory in turn 1 and injects it into a later request', async () => {
    const memory = new MockMemory({
      enableWorkingMemory: true,
      workingMemoryTemplate: `# User Profile\n- **Name**:\n- **Preference**:\n`,
    });
    const threadId = 'working-memory-thread';
    const resourceId = 'working-memory-resource';

    await memory.saveThread({
      thread: {
        id: threadId,
        title: 'WM Thread',
        resourceId,
        metadata: {},
        createdAt: new Date(),
        updatedAt: new Date(),
      },
    });

    const storedMemory = '# User Profile\n- **Name**: Ada\n- **Preference**: WM_DARK_MODE\n';

    // Turn 1: model calls updateWorkingMemory, then turn 2 of the inner loop
    // (with the tool result) emits final text.
    await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Remember that my name is Ada and I prefer WM_DARK_MODE.',
      memory,
      threadId,
      resourceId,
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          {
            toolCalls: [
              {
                id: 'call_wm',
                name: 'updateWorkingMemory',
                arguments: { memory: storedMemory },
              },
            ],
          },
        );
        llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Saved your profile.' });
      },
    });

    // The working memory must be persisted to the thread without exposing the
    // internal updateWorkingMemory invocation in message history.
    const savedWorkingMemory = await memory.getWorkingMemory({ threadId, resourceId });
    expect(savedWorkingMemory).toContain('WM_DARK_MODE');

    const { messages } = await memory.recall({ threadId, resourceId });
    expect(messages.map(message => message.role)).toEqual(['user', 'assistant']);
    expect(
      messages
        .flatMap(message => message.content.parts)
        .filter(part => part.type === 'tool-invocation' && part.toolInvocation.toolName === 'updateWorkingMemory'),
    ).toHaveLength(0);

    getMock().clearRequests();
    getMock().clearFixtures();
    getMock().resetMatchCounts();

    // Turn 2: a fresh run on the same thread. The stored working memory must be
    // injected into the request (system prompt) so the model can use it.
    const { requests } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'What do you know about me?',
      memory,
      threadId,
      resourceId,
      fixtures: llm => {
        llm.on({ endpoint: 'chat' }, { content: 'You are Ada and you prefer dark mode.' });
      },
    });

    expect(requests).toHaveLength(1);
    const serialized = JSON.stringify(requests[0]?.body?.messages ?? []);
    expect(serialized).toContain('WM_DARK_MODE');
  });

  // #28565: a working-memory signal before the first response must not give an
  // output processor ownership of the previous turn's stored reply.
  for (const method of ['stream', 'generate'] as const) {
    it.each([true, false])(
      `${method}: processing a new reply preserves the earlier reply (stored working memory: %s)`,
      async storedWorkingMemory => {
        const llm = getMock();
        const threadId = 'working-memory-response-thread';
        const resourceId = 'working-memory-response-resource';
        const memory = new Memory({
          storage: new InMemoryStore(),
          options: {
            generateTitle: false,
            semanticRecall: false,
            workingMemory: { enabled: true, scope: 'resource' },
            observationalMemory: {
              enabled: true,
              observation: {
                manageWorkingMemory: true,
                model: createOpenAI({ apiKey: 'aimock-test-key', baseURL: `${llm.url}/v1` }).chat(SCENARIO_MODEL_ID),
                messageTokens: 100_000,
                bufferTokens: false,
              },
              reflection: { observationTokens: 100_000 },
            },
          },
        });
        await memory.saveThread({
          thread: {
            id: threadId,
            resourceId,
            title: 'Earlier conversation',
            metadata: {},
            createdAt: new Date(1_000),
            updatedAt: new Date(1_000),
          },
        });
        const earlierReply: MastraDBMessage = {
          id: 'earlier-reply',
          role: 'assistant',
          threadId,
          resourceId,
          createdAt: new Date(2_000),
          content: {
            format: 2,
            metadata: { requestId: 'earlier-request' },
            parts: [
              {
                type: 'tool-invocation',
                toolInvocation: {
                  state: 'result',
                  toolCallId: 'earlier-lookup',
                  toolName: 'lookup',
                  args: {},
                  result: 'An earlier order',
                },
              },
              { type: 'text', text: 'Your earlier order shipped.' },
            ],
          },
        };
        await memory.saveMessages({ messages: [earlierReply] });
        const before = (await memory.recall({ threadId, resourceId })).messages.find(m => m.id === earlierReply.id)!;
        const earlierContent = structuredClone(before.content);
        if (storedWorkingMemory) {
          await memory.updateWorkingMemory({
            threadId,
            resourceId,
            workingMemory: '# Notes\n- Prefers email updates.',
          });
        }

        const processorMessages: string[][] = [];
        const tagReply: OutputProcessor = {
          id: 'tag-current-request',
          processOutputResult: async ({ messages }) => {
            processorMessages.push(messages.map(message => message.id));
            return messages.map(message => ({
              ...message,
              content: {
                ...message.content,
                metadata: { ...message.content.metadata, requestId: 'current-request' },
              },
            }));
          },
        };
        llm.on({ endpoint: 'chat' }, { content: 'Your new order is ready.' });
        const { agent } = await createSharedAgent(llm, { engine, memory });
        const result = await agent[method]('Check my new order.', {
          memory: { thread: threadId, resource: resourceId },
          outputProcessors: [tagReply],
          returnScorerData: true,
        });
        const output = result.output ?? result;
        let full;
        try {
          if (method === 'stream') await output.consumeStream();
          full = method === 'stream' ? await output.getFullOutput() : result;
        } finally {
          result.cleanup?.();
        }

        expect(full.text).toBe('Your new order is ready.');
        const { messages: saved } = await memory.recall({ threadId, resourceId, hideSignals: false });
        expect(saved.find(message => message.id === earlierReply.id)?.content).toEqual(earlierContent);
        const currentReplies = saved.filter(message => message.role === 'assistant' && message.id !== earlierReply.id);
        expect(currentReplies).toHaveLength(1);
        expect(currentReplies[0]?.content.metadata?.requestId).toBe('current-request');
        expect(processorMessages).toEqual([[currentReplies[0]!.id]]);
        // Durable outputs can dehydrate dbMessages; scoringData hydrates the run's
        // response from storage and must contain exactly the current reply there too.
        if (!isDurableEngineVariant(engine)) {
          expect(full.response.dbMessages.map((message: MastraDBMessage) => message.id)).toEqual([
            currentReplies[0]!.id,
          ]);
        }
        // Inngest does not forward returnScorerData to its stream adapter; the
        // independent expected failure below keeps that existing gap visible.
        if (engine !== 'inngest') {
          expect(full.scoringData.output.map((message: MastraDBMessage) => message.id)).toEqual([
            currentReplies[0]!.id,
          ]);
          expect(JSON.stringify(full.scoringData.output)).not.toContain('earlier-lookup');
        }
        expect(JSON.stringify(full.response.dbMessages)).not.toContain('earlier-lookup');
        expect(llm.getRequests()).toHaveLength(1);
        const prompt = JSON.stringify(llm.getRequests()[0]?.body.messages);
        expect(prompt).toContain('Your earlier order shipped.');
        if (storedWorkingMemory) expect(prompt).toContain('Prefers email updates.');
      },
    );
  }

  if (engine === 'inngest') {
    it.fails.each(['stream', 'generate'] as const)('%s returns scorer data when requested on Inngest', async method => {
      const llm = getMock();
      llm.on({ endpoint: 'chat' }, { content: 'The new order is ready.' });
      const { agent } = await createSharedAgent(llm, { engine });
      const result = await agent[method]('Check my order.', { returnScorerData: true });
      const output = result.output ?? result;
      try {
        if (method === 'stream') await output.consumeStream();
        const full = method === 'stream' ? await output.getFullOutput() : result;
        expect(full.text).toBe('The new order is ready.');
        expect(full.scoringData).toMatchObject({ output: expect.any(Array) });
      } finally {
        result.cleanup?.();
      }
    });
  }
});
