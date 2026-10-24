import { AGENT_CONTROL_TOPIC, DurableStepIds } from '@mastra/core/agent/durable';
import type { EventCallback, PubSub } from '@mastra/core/events';
import type { Inngest } from 'inngest';

export const INNGEST_AGENTIC_LOOP_ID = `inngest:${DurableStepIds.AGENTIC_LOOP}`;
const TERMINATED = 'inngest-execution-terminated';

interface ExecutionTermination {
  executionId: string;
  nativeRunId: string;
  cancelled: boolean;
  error: { name: string; message: string };
}

/** Forward only confirmed root-function termination, never an individual retry or nested failure. */
export function createExecutionLifecycleFunction(
  inngest: Inngest,
  functionId: string,
  getPubsub: () => PubSub,
): ReturnType<Inngest['createFunction']> {
  return inngest.createFunction(
    {
      id: `workflow.${INNGEST_AGENTIC_LOOP_ID}.lifecycle`,
      retries: 3,
      triggers: [
        { event: 'inngest/function.failed', if: `event.data.function_id == ${JSON.stringify(functionId)}` },
        { event: 'inngest/function.cancelled', if: `event.data.function_id == ${JSON.stringify(functionId)}` },
      ],
    },
    async ({ event, step }) => {
      const original = event.data.event;
      const runId = original?.data?.runId;
      const executionId = original?.data?.__mastraExecutionId;
      if (
        event.data.function_id !== functionId ||
        original?.name !== `workflow.${INNGEST_AGENTIC_LOOP_ID}` ||
        typeof runId !== 'string' ||
        typeof executionId !== 'string' ||
        typeof event.data.run_id !== 'string'
      )
        return;

      await step.run('notify-agent-execution-termination', async () => {
        await getPubsub().publish(AGENT_CONTROL_TOPIC(runId), {
          type: TERMINATED,
          runId,
          data: {
            executionId,
            nativeRunId: event.data.run_id,
            cancelled: event.name === 'inngest/function.cancelled',
            error: {
              name: event.data.error?.name ?? 'InngestExecutionError',
              message: event.data.error?.message ?? 'Inngest execution terminated',
            },
          } satisfies ExecutionTermination,
        });
      });
    },
  );
}

/** The live caller owns settlement. A previous execution cannot settle a later approval. */
export function watchExecutionLifecycle(
  pubsub: PubSub,
  runId: string,
  onTerminated: (event: ExecutionTermination) => Promise<void>,
) {
  const executionId = crypto.randomUUID();
  const topic = AGENT_CONTROL_TOPIC(runId);
  let disposed = false;
  let terminating: Promise<void> | undefined;
  const listener: EventCallback = async (event, ack) => {
    if (!disposed && event.type === TERMINATED && event.data?.executionId === executionId) {
      terminating ??= onTerminated(event.data);
      await terminating;
    }
    await ack?.();
  };
  const ready = pubsub.subscribe(topic, listener);
  // Setup failure is surfaced by the caller before it dispatches any work.
  void ready.catch(() => {});
  return {
    executionId,
    ready,
    dispose() {
      if (disposed) return;
      disposed = true;
      // A stream can settle while subscription setup is still completing.
      void ready.then(() => pubsub.unsubscribe(topic, listener)).catch(() => {});
    },
  };
}
