import { createRequire } from 'node:module';
import { expect, it } from 'vitest';
import { z } from 'zod/v4';
import { PostgresStore } from '../../../../stores/pg/src';
import { createOpenAI } from '../../../core/node_modules/@ai-sdk/openai-v5';
import {
  describeForAllEngines,
  useLoopScenarioAimock,
  runLoopScenario,
  createSharedAgent,
} from '../../../core/src/loop/test-utils/aimock/aimock-scenario';
import { createTool } from '../../../core/src/tools';
import { Memory } from '../../src';
const { Pool } = createRequire(new URL('../../../../stores/pg/package.json', import.meta.url))('pg');

describeForAllEngines('Durable memory read budget (#28366)', engine => {
  const getMock = useLoopScenarioAimock();
  // This shared scenario needs an explicitly configured real PostgreSQL database.
  const test = process.env.MEMORY_READ_DATABASE_URL ? it : it.skip;
  test('recalls stored working memory and observations with a bounded read budget', async () => {
    const llm = getMock();
    const schema = `memo_${crypto.randomUUID().replaceAll('-', '')}`;
    const pool = new Pool({ connectionString: process.env.MEMORY_READ_DATABASE_URL });
    const reads: string[] = [];
    let measuring = false;
    const delayMs = Number(process.env.MEMORY_READ_DELAY_MS ?? 0);
    pool.on('connect', (client: any) => {
      const original = client.query.bind(client);
      client.query = (...args: any[]) => {
        if (measuring) {
          reads.push(typeof args[0] === 'string' ? args[0] : args[0]?.text);
          if (delayMs) {
            // Added round-trip delay is a labelled simulation; PostgreSQL still executes every query.
            if (typeof args.at(-1) === 'function') {
              setTimeout(() => original(...args), delayMs);
              return;
            }
            return new Promise(resolve => setTimeout(resolve, delayMs)).then(() => original(...args));
          }
        }
        return original(...args);
      };
    });
    const storage = new PostgresStore({ id: schema, schemaName: schema, pool });
    try {
      await storage.init();
      const memory = new Memory({
        storage,
        options: {
          workingMemory: { enabled: true, scope: 'resource' },
          observationalMemory: {
            enabled: true,
            observation: {
              model: createOpenAI({ apiKey: 'test', baseURL: `${llm.url}/v1` }).chat('gpt-4o-mini'),
              messageTokens: 100000,
              bufferTokens: false,
            },
          },
        },
      });
      let threadId = `thread-${crypto.randomUUID()}`;
      const resourceId = `resource-${crypto.randomUUID()}`;
      await memory.createThread({ threadId, resourceId, title: 'Read budget' });
      await memory.updateWorkingMemory({ threadId, resourceId, workingMemory: 'PREFERENCE_PURPLE' });
      const samples: { beforeModelMs: number; queries: number; resourceReads: number; omReads: number }[] = [];
      const repetitions = Number(process.env.MEMORY_READ_REPEATS ?? 1);
      const warmups = repetitions >= 30 ? 5 : 0;
      for (let iteration = -warmups; iteration < repetitions; iteration++) {
        // Independent pre-existing threads avoid carrying unrelated response-merging
        // behavior between benchmark samples. Database/cache warmup is still real.
        if (repetitions > 1) {
          threadId = `thread-${crypto.randomUUID()}`;
          await memory.createThread({ threadId, resourceId, title: 'Read budget' });
          await (await memory.omEngine)!.getOrCreateRecord(threadId, resourceId);
        }
        llm.clearRequests();
        llm.clearFixtures();
        llm.resetMatchCounts();
        reads.length = 0;
        const expectedText = `Purple for turn ${iteration}.`;
        measuring = true;
        const start = performance.now();
        let beforeModelMs = 0;
        const result = await runLoopScenario({
          engine,
          llm,
          memory,
          threadId,
          resourceId,
          prompt: 'What is my preference?',
          fixtures: mock => {
            mock.on(
              {
                endpoint: 'chat',
                predicate: () => {
                  measuring = false;
                  beforeModelMs = performance.now() - start;
                  return true;
                },
              },
              { content: expectedText },
            );
          },
        });
        const resourceReads = reads.filter(sql => /SELECT/i.test(sql) && /mastra_resources/.test(sql)).length;
        const omReads = reads.filter(sql => /SELECT/i.test(sql) && /mastra_observational_memory/.test(sql)).length;
        if (iteration >= 0) samples.push({ beforeModelMs, queries: reads.length, resourceReads, omReads });
        expect(JSON.stringify(result.requests[0]?.body?.messages)).toContain('PREFERENCE_PURPLE');
        expect(await result.output.text).toBe(expectedText);
        if (repetitions === 1)
          console.log('MEMORY_READ_EVIDENCE', JSON.stringify({ engine, beforeModelMs, reads, resourceReads }));
        // Soft failures let the unchanged baseline finish the benchmark without hiding the regression.
        expect.soft(resourceReads, 'one resource row read shared by working and observational memory').toBe(1);
      }
      console.log('MEMORY_BENCHMARK_EVIDENCE', JSON.stringify({ engine, delayMs, warmups, samples }));
    } finally {
      measuring = false;
      await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      await pool.end();
    }
  });
  for (const approved of [false, true]) {
    test(`persists tool writes and reloads later-turn memory, including approval=${approved}`, async () => {
      const llm = getMock();
      const schema = `fresh_${crypto.randomUUID().replaceAll('-', '')}`;
      const pool = new Pool({ connectionString: process.env.MEMORY_READ_DATABASE_URL });
      const storage = new PostgresStore({ id: schema, schemaName: schema, pool });
      let output: any;
      try {
        await storage.init();
        const memory = new Memory({
          storage,
          options: {
            workingMemory: { enabled: true, scope: 'resource' },
            observationalMemory: {
              enabled: true,
              observation: {
                model: createOpenAI({ apiKey: 'test', baseURL: `${llm.url}/v1` }).chat('gpt-4o-mini'),
                messageTokens: 100000,
                bufferTokens: false,
              },
            },
          },
        });
        const threadId = `thread-${crypto.randomUUID()}`;
        const resourceId = `resource-${crypto.randomUUID()}`;
        await memory.createThread({ threadId, resourceId });
        await memory.updateWorkingMemory({ threadId, resourceId, workingMemory: 'FIRST_PURPLE' });
        const tool = createTool({
          id: 'change',
          description: 'Change the stored preference.',
          inputSchema: z.object({}),
          requireApproval: approved,
          execute: async () => {
            await memory.updateWorkingMemory({ threadId, resourceId, workingMemory: 'SECOND_GREEN' });
            return { changed: true };
          },
        });
        const shared = await createSharedAgent(llm, { engine, memory, tools: { change: tool } });
        const initial = await runLoopScenario({
          engine,
          llm,
          memory,
          sharedAgent: shared,
          threadId,
          resourceId,
          prompt: 'Change my preference.',
          fixtures: mock => {
            mock.on(
              { endpoint: 'chat', hasToolResult: false },
              { toolCalls: [{ id: 'call_change', name: 'change', arguments: {} }] },
            );
            mock.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Changed.' });
          },
        });
        if (approved) {
          await memory.updateWorkingMemory({ threadId, resourceId, workingMemory: 'BETWEEN_APPROVAL_BLUE' });
          output = await shared.agent.approveToolCall({ runId: initial.output.runId, toolCallId: 'call_change' });
          for await (const chunk of output.fullStream) {
            /* drain real resumed output */
          }
        }
        const requests = llm.getRequests();
        expect(requests).toHaveLength(2);
        expect(JSON.stringify(requests[0]?.body?.messages)).toContain('FIRST_PURPLE');
        // A direct memory write does not re-inject the prompt mid-turn. Check
        // the stored write and the next turn, which does reload working memory.
        expect(await memory.getWorkingMemory({ threadId, resourceId })).toContain('SECOND_GREEN');
        llm.clearRequests();
        llm.clearFixtures();
        llm.resetMatchCounts();
        await memory.updateWorkingMemory({ threadId, resourceId, workingMemory: 'THIRD_ORANGE' });
        const next = await runLoopScenario({
          engine,
          llm,
          memory,
          sharedAgent: shared,
          threadId,
          resourceId,
          prompt: 'What is my new preference?',
          fixtures: mock => mock.on({ endpoint: 'chat' }, { content: 'Orange.' }),
        });
        expect(JSON.stringify(next.requests[0]?.body?.messages)).toContain('THIRD_ORANGE');
        // Cached reads must not let a different resource use an owned thread.
        await expect(
          shared.agent.generate('Read it', { memory: { thread: threadId, resource: 'foreign-resource' } }),
        ).rejects.toThrow();
      } finally {
        output?.cleanup?.();
        await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
        await pool.end();
      }
    });
  }
});
