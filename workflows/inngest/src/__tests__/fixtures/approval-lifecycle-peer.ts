import { createOpenAI } from '@ai-sdk/openai-v5';
import { serve as serveHttp } from '@hono/node-server';
import { Agent } from '@mastra/core/agent';
import { Mastra } from '@mastra/core/mastra';
import { createTool } from '@mastra/core/tools';
import { DefaultStorage } from '@mastra/libsql';
import { Memory } from '@mastra/memory';
import { Hono } from 'hono';
import { Inngest } from 'inngest';
import { z } from 'zod/v4';
import Redis from '../../../../../stores/redis/node_modules/ioredis/built/index.js';
import { RedisServerCache } from '../../../../../stores/redis/src/cache';
import type { InngestAgent } from '../../durable-agent';
import { createInngestAgent } from '../../durable-agent';
import { serve as inngestServe } from '../../index';

export interface PeerConfig {
  id: string;
  dbUrl: string;
  modelUrl: string;
  port: number;
  mode: 'cancel' | 'exhausted';
}
export function buildLifecyclePeer(
  config: PeerConfig,
  worker = false,
): { agent: InngestAgent; mastra: Mastra; inngest: Inngest; release: () => void; close: () => Promise<unknown> } {
  let release!: () => void;
  const held = new Promise<void>(resolve => {
    release = resolve;
  });
  const inngest = new Inngest({ id: config.id, isDev: true, baseUrl: `http://127.0.0.1:${config.port}` });
  const redis = new Redis(process.env.AIMOCK_APPROVAL_REDIS_URL!, { maxRetriesPerRequest: 2 });
  const serverCache = new RedisServerCache({ client: redis }, { keyPrefix: `${config.id}:` });
  const storage = new DefaultStorage({ id: config.id, url: config.dbUrl });
  const tool = createTool({
    id: 'operate',
    description: 'An approved operation on the remote worker.',
    inputSchema: z.object({}),
    requireApproval: true,
    execute: async () => {
      if (!worker) throw new Error('Tool incorrectly ran in the caller process');
      process.send?.({ type: 'tool-started', pid: process.pid });
      await held;
      process.send?.({ type: 'tool-ended', pid: process.pid });
      return { done: true };
    },
  });
  const original = new Agent({
    id: config.id,
    name: 'Remote approval lifecycle',
    instructions: 'Use operate, then confirm.',
    model: createOpenAI({ apiKey: 'test', baseURL: `${config.modelUrl}/v1` }).chat('gpt-4o-mini'),
    tools: { operate: tool },
    memory: new Memory({ storage }),
  });
  const agent = createInngestAgent({ agent: original, inngest, retries: 2 });
  const mastra = new Mastra({ agents: { [config.id]: agent } as any, storage, cache: serverCache, logger: false });
  return { agent, mastra, inngest, release, close: () => redis.quit() };
}
if (process.argv[2] === 'approval-lifecycle-peer') {
  const config: PeerConfig = JSON.parse(process.argv[3]!);
  const peer = buildLifecyclePeer(config, true);
  process.on('message', message => {
    if ((message as any)?.type === 'release') peer.release();
  });
  const handler = inngestServe({ mastra: peer.mastra, inngest: peer.inngest });
  const app = new Hono();
  app.all('/api/inngest', handler);
  const server = serveHttp({
    port: config.port + 1,
    fetch: async request => {
      if (request.method === 'POST') {
        const body = await request.clone().json();
        if (body.event?.name === 'workflow.inngest:durable-agentic-loop' && body.event.data?.resume) {
          process.send?.({ type: 'execution', runId: body.ctx.run_id, attempt: body.ctx.attempt, pid: process.pid });
          if (config.mode === 'exhausted')
            return new Response(
              JSON.stringify({ name: 'SelectedWorkerFailure', message: 'Selected remote request unavailable' }),
              {
                status: 503,
                headers: { 'content-type': 'application/json', 'x-inngest-no-retry': 'false' },
              },
            );
        }
      }
      return app.fetch(request);
    },
  });
  await fetch(`http://127.0.0.1:${config.port + 1}/api/inngest`, { method: 'PUT' });
  process.send?.({ type: 'ready', pid: process.pid });
  process.on('SIGTERM', () => {
    peer.release();
    server.close();
    void peer.close().finally(() => process.exit(0));
  });
}
