/**
 * Runs core's aimock `describeForAllEngines` scenarios on the Inngest engine.
 *
 * Every scenario agent is wrapped with `createInngestAgent` and added to the shared Mastra that
 * the Inngest dev server calls back into, so each run goes through a real Inngest function.
 *
 * Each test worker starts its own dev server + app server once (on its own ports) and reuses
 * them for every file it runs. Workers can't share one dev server: every worker serves the same
 * agentic workflow, so the dev server would hand a run to whichever worker it picked.
 */
import { createServer } from 'node:http';
import type { Agent } from '@mastra/core/agent';
import { setScenarioEngineHost } from '@mastra/core/loop/test-utils/aimock/aimock-scenario';
import { Inngest } from 'inngest';
import { beforeAll } from 'vitest';
import { approvalLifecycleHost, LifecycleDeliveryPubSub } from '../approval-lifecycle-host';

// Start at 5100+ so worker ports stay clear of the default test ports (4100/4101) and `mastra dev` (4111).
const PORTS_PER_WORKER = 10;
process.env.INNGEST_TEST_PORT_OFFSET ??= String(1000 + PORTS_PER_WORKER * Number(process.env.VITEST_POOL_ID ?? 1));

// Imported after the port offset is set: the test utils read it at load time.
const { createInngestAgent } = await import('../../durable-agent');
const { getSharedInngest, getSharedMastra, setupSharedTestInfrastructure, teardownSharedTestInfrastructure } =
  await import('../durable-agent.test.utils');

setScenarioEngineHost({
  engine: 'inngest',
  approvalLifecycle: process.env.AIMOCK_APPROVAL_LIFECYCLE === '1' ? approvalLifecycleHost : undefined,
  getMastra: getSharedMastra,
  wrapAgent: (agent: Agent) =>
    createInngestAgent({
      agent,
      inngest: getSharedInngest(),
      ...(process.env.AIMOCK_APPROVAL_LIFECYCLE === '1'
        ? {
            pubsub: new LifecycleDeliveryPubSub(getSharedInngest(), 'inngest:durable-agentic-loop'),
          }
        : {}),
    }),
  withEventSendFailure: async action => {
    const requests: unknown[] = [];
    const server = createServer((request, response) => {
      let body = '';
      request.on('data', chunk => {
        body += chunk;
      });
      request.on('end', () => {
        requests.push(JSON.parse(body));
        response.writeHead(400, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ status: 400, error: 'fixture dispatch unavailable' }));
      });
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('HTTP fault fixture failed to listen');
    const client = getSharedInngest();
    const faultClient = new Inngest({
      id: 'aimock-event-send-fault',
      baseUrl: `http://127.0.0.1:${address.port}`,
      isDev: true,
    });
    const send = client.send;
    client.send = faultClient.send.bind(faultClient);
    try {
      return await action();
    } finally {
      client.send = send;
      await new Promise<void>((resolve, reject) => server.close(error => (error ? reject(error) : resolve())));
      console.log('RESUME_DISPATCH_FAULT', JSON.stringify({ requests }));
      if (requests.length === 0) throw new Error('The dispatch fault fixture received no HTTP requests');
    }
  },
});

const globalState = globalThis as { __inngestAimockReady?: Promise<void> };

beforeAll(async () => {
  if (!globalState.__inngestAimockReady) {
    globalState.__inngestAimockReady = setupSharedTestInfrastructure({
      backgroundTasks: { enabled: true, globalConcurrency: 5, perAgentConcurrency: 3 },
      ...(process.env.AIMOCK_APPROVAL_LIFECYCLE === '1' ? { retries: 2, fastRetries: true } : {}),
    });
    process.once('beforeExit', () => void teardownSharedTestInfrastructure());
  }
  await globalState.__inngestAimockReady;
}, 60_000);
