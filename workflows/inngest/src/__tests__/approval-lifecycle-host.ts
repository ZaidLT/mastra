import { CachingPubSub } from '@mastra/core/events';
import type { Event, EventCallback } from '@mastra/core/events';
import type { Inngest, InngestFunction } from 'inngest';
import type { ScenarioEngineHost } from '../../../../packages/core/src/loop/test-utils/aimock/types';
import { InngestPubSub } from '../pubsub';

type Mode = 'cancel' | 'exhausted' | 'transient' | 'healthy' | 'normal-error' | 'stale' | 'delayed-finish';
type RequestRecord = {
  nativeRunId: string;
  eventId: string;
  executionId?: string;
  eventName: string;
  attempt: number;
  stepId: string;
  status?: number;
  injected: boolean;
};
type State = {
  runId: string;
  mode: Mode;
  nativeRunId?: string;
  requests: RequestRecord[];
  active: number;
  released: boolean;
  release(): void;
  gate: Promise<void>;
  deliveryGate: Promise<void>;
  releaseDelivery(): void;
  heldDeliveries: number;
  lifecycleGate: Promise<void>;
  releaseLifecycle(): void;
  lifecycleRequests: number;
  lifecycleDeliveries: number;
  retryGate: Promise<void>;
  releaseRetry(): void;
  terminal?: unknown;
  notifications: unknown[];
  publications: unknown[];
};
const states = new Map<string, State>();
const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function until<T>(
  read: () => T | Promise<T>,
  accept: (value: T) => boolean,
  label: string,
  timeout = 15_000,
): Promise<T> {
  const deadline = Date.now() + timeout;
  let result: T;
  do {
    result = await read();
    if (accept(result)) return result;
    await wait(50);
  } while (Date.now() < deadline);
  throw new Error(`Lifecycle fixture timed out: ${label}; last=${JSON.stringify(result)}`);
}

async function gql(query: string, variables: Record<string, unknown> = {}) {
  const port = 4100 + Number(process.env.INNGEST_TEST_PORT_OFFSET ?? 0);
  const response = await fetch(`http://127.0.0.1:${port}/v0/gql`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ query, variables }),
  });
  const result = (await response.json()) as any;
  if (result.errors?.every((e: any) => e.message.includes('no rows in result set'))) return result.data;
  if (!response.ok || result.errors) throw new Error(`Inngest API: ${JSON.stringify(result)}`);
  return result.data;
}

async function readRun(id: string) {
  const data = await gql('query($id: String!) { run(runID: $id) { id status endedAt triggerIDs eventName } }', { id });
  return data.run;
}

export const approvalLifecycleHost: NonNullable<ScenarioEngineHost['approvalLifecycle']> = {
  remote: async (mode, url) =>
    (await import('./approval-lifecycle-remote')).remoteApprovalLifecycle(
      mode,
      url,
      4100 + Number(process.env.INNGEST_TEST_PORT_OFFSET ?? 0),
    ),
  arm(runId, mode) {
    let open!: () => void;
    const gate = new Promise<void>(resolve => {
      open = resolve;
    });
    let releaseRetry!: () => void;
    const retryGate = new Promise<void>(resolve => {
      releaseRetry = resolve;
    });
    let releaseDelivery!: () => void;
    const deliveryGate = new Promise<void>(resolve => {
      releaseDelivery = resolve;
    });
    let releaseLifecycle!: () => void;
    const lifecycleGate = new Promise<void>(resolve => {
      releaseLifecycle = resolve;
    });
    const state: State = {
      lifecycleGate,
      releaseLifecycle,
      lifecycleRequests: 0,
      lifecycleDeliveries: 0,
      deliveryGate,
      releaseDelivery,
      heldDeliveries: 0,
      runId,
      mode,
      requests: [],
      notifications: [],
      publications: [],
      active: 0,
      released: false,
      gate,
      retryGate,
      releaseRetry,
      release: () => {
        state.released = true;
        open();
      },
    };
    states.set(runId, state);
    return {
      waitForRequest: async () => {
        await until(() => state.nativeRunId, Boolean, 'resumed native request');
      },
      waitForRetry: async () => {
        await until(
          () => state.requests.some(r => r.nativeRunId === state.nativeRunId && r.attempt > 0),
          Boolean,
          'real retry request',
        );
      },
      waitForLifecycleRequest: async () => {
        await until(
          () => state.lifecycleRequests,
          n => n > 0,
          'held lifecycle request',
        );
      },
      waitForLifecycleDelivery: async () => {
        await until(
          () => state.lifecycleDeliveries,
          n => n > 0,
          'lifecycle delivery',
        );
      },
      waitForNotification: async () => {
        await until(
          () => state.notifications.length,
          n => n > 0,
          'native notification',
        );
      },
      releaseLifecycle: state.releaseLifecycle,
      releaseDelivery: state.releaseDelivery,
      waitForHeldDelivery: async () => {
        await until(
          () => state.heldDeliveries,
          n => n > 0,
          'held real terminal delivery',
        );
      },
      releaseRetry: state.releaseRetry,
      release: state.release,
      cancel: async () => {
        if (!state.nativeRunId) throw new Error('No native execution to cancel');
        await gql('mutation($id: ULID!) { cancelRun(runID: $id) { id status } }', { id: state.nativeRunId });
      },
      waitForTerminal: async () => {
        if (!state.nativeRunId) throw new Error('No native execution to inspect');
        const terminal = await until(
          () => readRun(state.nativeRunId!),
          run => ['FAILED', 'CANCELLED', 'COMPLETED'].includes(run?.status),
          'native terminal status',
          25_000,
        );
        await until(
          () => state.active,
          value => value === 0,
          'worker requests drained',
        );
        state.terminal = terminal;
        if (state.mode === 'cancel' || state.mode === 'exhausted' || state.mode === 'stale')
          await until(
            () => state.notifications.length,
            count => count > 0,
            'native terminal notification',
          );
        return terminal;
      },
      snapshot: () => ({
        runId,
        mode,
        nativeRunId: state.nativeRunId,
        requests: state.requests.map(r => ({ ...r })),
        activeRequests: state.active,
        lifecycleRequests: state.lifecycleRequests,
        lifecycleDeliveries: state.lifecycleDeliveries,
        heldDeliveries: state.heldDeliveries,
        terminal: state.terminal,
        notifications: [...state.notifications],
        publications: [...state.publications],
      }),
      cleanup: async () => {
        state.release();
        state.releaseRetry();
        state.releaseLifecycle();
        state.releaseDelivery();
        await until(
          () => state.active,
          value => value === 0,
          'fixture cleanup',
          5_000,
        );
        states.delete(runId);
      },
    };
  },
};

/** Real HTTP fault injection. All unrelated runs, sync calls and failure handlers pass through. */
export async function interceptApprovalLifecycleRequest(
  request: Request,
  next: (request: Request) => Response | Promise<Response>,
): Promise<Response> {
  if (request.method !== 'POST') return next(request);
  let body: any;
  try {
    body = await request.clone().json();
  } catch {
    return next(request);
  }
  const event = body.event;
  // Hold an actual lifecycle delivery for the terminated execution while a later approval runs.
  if (
    ['inngest/function.failed', 'inngest/function.cancelled'].includes(event?.name) &&
    new URL(request.url).searchParams.get('fnId')?.endsWith('durable-agentic-loop.lifecycle')
  ) {
    const original = event.data?.event;
    const state = states.get(original?.data?.runId);
    if (state?.mode === 'stale' && state.nativeRunId === event.data?.run_id) {
      state.lifecycleRequests++;
      await state.lifecycleGate;
      const response = await next(request);
      const operations = response.status === 206 ? await response.clone().json() : [];
      if (response.status === 200 || (Array.isArray(operations) && operations.some(op => op.op === 'RunComplete')))
        state.lifecycleDeliveries++;
      return response;
    }
  }
  const state = states.get(event?.data?.runId) ?? states.get(event?.data?.inputData?.runId);
  if (!state || event?.name !== 'workflow.inngest:durable-agentic-loop' || !event?.data?.resume) return next(request);
  const nativeRunId = body.ctx?.run_id;
  if (!nativeRunId) throw new Error(`No native ID in SDK request: ${JSON.stringify(body).slice(0, 1000)}`);
  state.nativeRunId ??= nativeRunId;
  // The second approval may create a new invocation. Observe it, but never apply the old fault to it.
  const target = nativeRunId === state.nativeRunId;
  const inject = target && (state.mode === 'exhausted' || (state.mode === 'transient' && body.ctx?.attempt === 0));
  const record: RequestRecord = {
    nativeRunId,
    eventId: event.id,
    executionId: event.data?.__mastraExecutionId,
    eventName: event.name,
    attempt: body.ctx?.attempt ?? 0,
    stepId: new URL(request.url).searchParams.get('stepId') ?? '',
    injected: inject,
  };
  state.requests.push(record);
  state.active++;
  try {
    if (inject) {
      // Hold the failing request until the test verifies the live claim.
      if (!state.released) await state.gate;
      record.status = 503;
      return new Response(
        JSON.stringify({ name: 'LifecycleFixtureWorkerUnavailable', message: 'Selected worker request unavailable' }),
        { status: 503, headers: { 'content-type': 'application/json', 'x-inngest-no-retry': 'false' } },
      );
    }
    if (target && state.mode === 'transient' && record.attempt > 0) await state.retryGate;
    const response = await next(request);
    const operations = target && state.mode === 'stale' && response.status === 206 ? await response.clone().json() : [];
    const completed =
      response.status === 200 || (Array.isArray(operations) && operations.some(op => op.op === 'RunComplete'));
    // The suspension and its snapshot are already delivered. Lose only the final
    // HTTP acknowledgement so native retries exhaust after the Mastra segment parks.
    if (target && state.mode === 'stale' && completed) {
      record.status = 503;
      record.injected = true;
      return new Response(
        JSON.stringify({ name: 'LostFinalAcknowledgement', message: 'Selected final response unavailable' }),
        {
          status: 503,
          headers: { 'content-type': 'application/json', 'x-inngest-no-retry': 'false' },
        },
      );
    }
    record.status = response.status;
    return response;
  } finally {
    state.active--;
  }
}

/** A passive test observer records system events without publishing Mastra events or releasing claims. */
export function createApprovalLifecycleObserver(inngest: Inngest): InngestFunction.Like {
  return inngest.createFunction(
    {
      id: 'approval-lifecycle-audit',
      retries: 0,
      triggers: [{ event: 'inngest/function.failed' }, { event: 'inngest/function.cancelled' }],
    },
    async ({ event }) => {
      for (const state of states.values()) {
        if (event.data.run_id === state.nativeRunId)
          state.notifications.push({
            name: event.name,
            nativeRunId: event.data.run_id,
            functionId: event.data.function_id,
            eventId: event.id,
            originalEventId: event.data.event?.id,
          });
      }
      return { recorded: true };
    },
  );
}

/** Observe actual publications while retaining the real cache and Inngest transport. */
export class LifecycleAuditedPubSub extends CachingPubSub {
  override async publish(topic: string, event: Event): Promise<void> {
    const state = states.get(event.runId ?? '');
    if (state && topic.startsWith('agent.stream.'))
      state.publications.push({
        topic,
        type: event.type,
        runId: event.runId,
        chunkType: (event.data as any)?.type,
        toolCallId: (event.data as any)?.payload?.toolCallId,
      });
    return super.publish(topic, event);
  }
}

/** Delay an actual realtime delivery, after publication, without replacing the transport. */
export class LifecycleDeliveryPubSub extends InngestPubSub {
  private wrappers = new Map<EventCallback, EventCallback>();
  override async subscribe(topic: string, callback: EventCallback): Promise<void> {
    const wrapped: EventCallback = async (...args) => {
      const event = args[0];
      const state = states.get(event.runId);
      if (state?.mode === 'delayed-finish' && event.type === 'finish') {
        state.heldDeliveries++;
        await state.deliveryGate;
      }
      await callback(...args);
    };
    this.wrappers.set(callback, wrapped);
    return super.subscribe(topic, wrapped);
  }
  override async unsubscribe(topic: string, callback: EventCallback): Promise<void> {
    const wrapped = this.wrappers.get(callback) ?? callback;
    this.wrappers.delete(callback);
    return super.unsubscribe(topic, wrapped);
  }
}
