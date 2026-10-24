import { expect, it } from 'vitest';
import { z } from 'zod/v4';
import { markResumeInFlight } from '../../../agent/durable/run-registry';
import { MockMemory } from '../../../memory';
import { createTool } from '../../../tools';
import {
  createSharedAgent,
  describeForAllEngines,
  getApprovalLifecycleHost,
  runLoopScenario,
  useLoopScenarioAimock,
} from './aimock-scenario';

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
function gate() {
  let release!: () => void;
  const promise = new Promise<void>(resolve => {
    release = resolve;
  });
  return { promise, release };
}
async function within<T>(promise: Promise<T>, ms = 10_000): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('Lifecycle diagnostic deadline')), ms);
      }),
    ]);
  } finally {
    clearTimeout(timer!);
  }
}
function probe(runId: string, callId: string) {
  const release = markResumeInFlight(runId, callId);
  if (!release) return 'held';
  release();
  return 'available';
}
function errorRecord(error: any) {
  return { id: error?.id, message: error?.message ?? String(error) };
}

/** Opt-in diagnostic cases extend the existing approval scenario on every supported engine. */
export function registerApprovalLifecycleDiagnostics() {
  if (process.env.AIMOCK_APPROVAL_LIFECYCLE !== '1') return;
  registerLifecycleEdges();
  describeForAllEngines('Approval lifecycle diagnostics (#27485)', engine => {
    const getMock = useLoopScenarioAimock();
    const repeats = Number(process.env.AIMOCK_LIFECYCLE_REPEATS ?? 1);
    const modes = ['cancel', 'exhausted', 'transient', 'healthy', 'normal-error'] as const;
    for (const mode of modes) {
      const native = ['cancel', 'exhausted', 'transient'].includes(mode);
      const supported = !native || engine === 'inngest';
      for (let repetition = 1; repetition <= (native && mode !== 'transient' ? repeats : 1); repetition++) {
        // Native cancellation and retry injection require the Inngest host capability.
        const test = supported ? it : it.skip;
        test(`${mode}: execution ends and releases its approval claim (${repetition})`, async () => {
          const llm = getMock();
          const toolStarted = gate();
          const toolGate = gate();
          const toolFinished = gate();
          const callbacks: string[] = [];
          const chunks: any[] = [];
          let executions = 0;
          let streamEnded = false;
          let streamFailure: unknown;
          const callId = `call_lifecycle_${mode}`;
          const tool = createTool({
            id: 'archive',
            description: 'Archive an item after approval',
            requireApproval: true,
            inputSchema: z.object({ item: z.string() }),
            execute: async () => {
              executions++;
              toolStarted.release();
              if (mode === 'cancel' || mode === 'healthy') await toolGate.promise;
              toolFinished.release();
              return { archived: true };
            },
          });
          const memory = new MockMemory();
          const shared = await createSharedAgent(llm, { tools: { archive: tool }, memory, engine });
          const initial = await runLoopScenario({
            engine,
            llm,
            sharedAgent: shared,
            memory,
            prompt: 'Archive A.',
            threadId: `lifecycle-${crypto.randomUUID()}`,
            resourceId: 'lifecycle-diagnosis',
            collectChunks: true,
            fixtures: mock => {
              mock.on(
                { endpoint: 'chat', hasToolResult: false },
                { toolCalls: [{ id: callId, name: 'archive', arguments: { item: 'A' } }] },
              );
              mock.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Archived.' });
            },
          });
          expect(initial.chunks!.some(chunk => chunk.type === 'tool-call-approval')).toBe(true);
          const runId = initial.output.runId!;
          const host = engine === 'inngest' ? getApprovalLifecycleHost()!.arm(runId, mode) : undefined;
          let result: any;
          let second: any;
          let reader: ReadableStreamDefaultReader<any> | undefined;
          let drained: Promise<void> | undefined;
          const record: any = {
            revision: process.env.AIMOCK_LIFECYCLE_REVISION,
            pid: process.pid,
            engine,
            mode,
            repetition,
            runId,
            callId,
            callbacks,
            chunks,
          };
          const approveAgain = async () => {
            const started = performance.now();
            try {
              second = await within(shared.agent.approveToolCall({ runId, toolCallId: callId }), 12_000);
              return { status: 'resolved', elapsedMs: performance.now() - started };
            } catch (error) {
              return { status: 'rejected', error: errorRecord(error), elapsedMs: performance.now() - started };
            }
          };
          try {
            if (mode === 'normal-error')
              llm.nextRequestError(400, {
                message: 'Lifecycle normal model error',
                type: 'invalid_request_error',
                code: 'invalid_request',
              });
            const resumeOptions = {
              runId,
              toolCallId: callId,
              onFinish: () => {
                callbacks.push('finish');
              },
              onError: () => {
                callbacks.push('error');
              },
              onAbort: () => {
                callbacks.push('abort');
              },
              onSuspended: () => {
                callbacks.push('suspended');
              },
            };
            result = await within(shared.agent.approveToolCall(resumeOptions));
            reader = result.fullStream.getReader();
            drained = (async () => {
              try {
                while (true) {
                  const item = await reader!.read();
                  if (item.done) break;
                  chunks.push({
                    type: item.value.type,
                    toolCallId: item.value.payload?.toolCallId,
                    text: item.value.payload?.text,
                  });
                }
              } catch (error) {
                streamFailure = errorRecord(error);
              } finally {
                streamEnded = true;
              }
            })();
            if (host) await host.waitForRequest();
            if (mode === 'cancel' || mode === 'healthy') {
              await within(toolStarted.promise);
              record.claimWhileRunning = probe(runId, callId);
              if (mode === 'cancel') await host!.cancel();
              else record.duplicateWhileRunning = await approveAgain();
              toolGate.release();
              await within(toolFinished.promise);
            }
            if (mode === 'exhausted' || mode === 'transient') {
              record.claimWhileRunning = probe(runId, callId);
              host!.release();
            }
            if (mode === 'transient') {
              await host!.waitForRetry();
              record.claimWhileRetrying = probe(runId, callId);
              host!.releaseRetry();
            }
            const terminal = host ? await host.waitForTerminal() : undefined;
            if (terminal) record.nativeTerminal = terminal;
            const deadline = Date.now() + 2_000;
            while (!streamEnded && Date.now() < deadline) await wait(25);
            await shared.agent.getPubSub?.()?.flush?.();
            record.beforeCleanup = {
              streamEnded,
              streamFailure,
              executions,
              claim: probe(runId, callId),
              callbacks: [...callbacks],
              modelRequests: llm.getRequests().length,
              native: host?.snapshot(),
            };
            if (mode === 'cancel' || mode === 'exhausted' || mode === 'normal-error')
              record.secondApproval = await approveAgain();
            console.log('APPROVAL_LIFECYCLE_EVIDENCE', JSON.stringify(record));
            if (mode === 'transient')
              expect
                .soft(record.claimWhileRetrying, 'claim stays held after a failed attempt while retrying')
                .toBe('held');
            if (mode === 'cancel') expect(terminal.status, 'native cancellation confirmed').toBe('CANCELLED');
            if (mode === 'exhausted') {
              expect(terminal.status, 'native failure confirmed').toBe('FAILED');
              expect(
                host!
                  .snapshot()
                  .requests.filter((r: any) => r.injected)
                  .map((r: any) => r.attempt),
                'two retries after first failure',
              ).toEqual([0, 1, 2]);
              expect(executions, 'failed worker never executes the tool').toBe(0);
            }
            expect.soft(record.beforeCleanup.streamEnded, 'stream settles after native termination').toBe(true);
            expect.soft(record.beforeCleanup.claim, 'claim released before caller cleanup').toBe('available');
            if (mode === 'cancel')
              expect
                .soft(
                  callbacks.filter(c => c === 'abort'),
                  'one cancellation callback',
                )
                .toHaveLength(1);
            if (mode === 'exhausted' || mode === 'normal-error')
              expect
                .soft(
                  callbacks.filter(c => c === 'error'),
                  'one error callback',
                )
                .toHaveLength(1);
            if (mode === 'transient' || mode === 'healthy') {
              expect.soft(executions, 'successful operation executes once').toBe(1);
              expect
                .soft(
                  callbacks.filter(c => c === 'finish'),
                  'one completion callback',
                )
                .toHaveLength(1);
            }
            if (engine === 'inngest' && (mode === 'healthy' || mode === 'transient'))
              expect
                .soft(record.claimWhileRunning, 'claim stays held while the execution is active or retrying')
                .toBe('held');
            if (mode === 'healthy')
              expect
                .soft(record.duplicateWhileRunning.error?.id, 'active execution remains protected')
                .toBe('AGENT_RESUME_ALREADY_CLAIMED');
          } finally {
            toolGate.release();
            host?.release();
            host?.releaseRetry();
            try {
              if (second?.fullStream) {
                const secondReader = second.fullStream.getReader();
                try {
                  await within(
                    (async () => {
                      while (!(await secondReader.read()).done) {
                        /* drain the real second resume */
                      }
                    })(),
                    12_000,
                  );
                } finally {
                  await secondReader.cancel().catch(() => {});
                }
              }
            } finally {
              try {
                result?.cleanup?.();
                second?.cleanup?.();
                await reader?.cancel().catch(() => {});
                if (drained) await within(drained, 2_000).catch(() => {});
              } finally {
                await host?.cleanup();
              }
            }
          }
        }, 45_000);
      }
    }
  });
}

function registerLifecycleEdges() {
  describeForAllEngines('Approval lifecycle delivery boundaries (#27485)', engine => {
    const getMock = useLoopScenarioAimock();
    const test = engine === 'inngest' ? it : it.skip;
    for (const mode of ['cancel', 'exhausted'] as const) {
      // Cross-process replay requires a real shared cache in addition to Inngest.
      const remoteTest = process.env.AIMOCK_APPROVAL_REDIS_URL ? test : it.skip;
      for (let repetition = 1; repetition <= Number(process.env.AIMOCK_LIFECYCLE_REMOTE_REPEATS ?? 1); repetition++) {
        remoteTest(
          `remote worker ${mode} releases the caller's claim (${repetition})`,
          async () => {
            const llm = getMock();
            llm.on(
              { endpoint: 'chat', hasToolResult: false },
              { toolCalls: [{ id: 'remote_call', name: 'operate', arguments: {} }] },
            );
            llm.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Done.' });
            const result = await getApprovalLifecycleHost()!.remote!(mode, llm.url);
            console.log('REMOTE_LIFECYCLE_EVIDENCE', JSON.stringify(result));
            expect(result.workerPid).not.toBe(result.callerPid);
            expect(result.claimDuringExecution).toBe('held');
            expect(result.claimAfterTermination).toBe('available');
            expect(result.settled).toBe(true);
            expect(result.callbacks).toEqual([mode === 'cancel' ? 'abort' : 'error']);
            expect(result.messages.filter((m: any) => m.type === 'tool-started')).toHaveLength(
              mode === 'cancel' ? 1 : 0,
            );
          },
          90000,
        );
      }
    }
    for (const mode of ['stale', 'cancel', 'delayed-finish'] as const) {
      for (let repetition = 1; repetition <= Number(process.env.AIMOCK_LIFECYCLE_EDGE_REPEATS ?? 1); repetition++) {
        test(`${mode}: termination belongs to its execution and keeps delivery intact (${repetition})`, async () => {
          const llm = getMock();
          const started = gate();
          const held = gate();
          const ended = gate();
          const notified = gate();
          const callbacks: string[] = [];
          const stages: number[] = [];
          const outputs: any[] = [];
          const observations: any[] = [];
          const tasks: Promise<any>[] = [];
          const callId = 'call_delivery';
          const tool = createTool({
            id: 'operate',
            description: 'Complete an approved operation.',
            inputSchema: z.object({ item: z.string() }),
            requireApproval: true,
            execute: async (_input, context) => {
              const stage = (context?.agent?.resumeData as { stage?: number })?.stage ?? 1;
              stages.push(stage);
              if (mode === 'stale' && stage === 1) return context?.agent?.suspend({ nextStage: 2 });
              started.release();
              if (mode !== 'delayed-finish') await held.promise;
              ended.release();
              return { done: true };
            },
          });
          const memory = new MockMemory();
          const shared = await createSharedAgent(llm, { engine, memory, tools: { operate: tool } });
          const initial = await runLoopScenario({
            engine,
            llm,
            sharedAgent: shared,
            memory,
            prompt: 'Operate on a.',
            threadId: `delivery-${crypto.randomUUID()}`,
            resourceId: 'lifecycle-delivery',
            collectChunks: true,
            fixtures: mock => {
              mock.on(
                { endpoint: 'chat', hasToolResult: false },
                {
                  toolCalls: [{ id: callId, name: 'operate', arguments: { item: 'a' } }],
                },
              );
              // Repeated text is legitimate and must remain unchanged.
              mock.on({ endpoint: 'chat', hasToolResult: true }, { content: 'Done.Done.' });
            },
          });
          const runId = initial.output.runId!;
          const host = getApprovalLifecycleHost()!.arm(runId, mode);
          const drain = (output: any) => {
            const task = (async () => {
              const chunks: any[] = [];
              for await (const chunk of output.fullStream) chunks.push(chunk);
              return chunks;
            })();
            // Cleanup still awaits it, but a deadline failure must not leave an unhandled rejection.
            void task.catch(() => {});
            tasks.push(task);
            return task;
          };
          const options = {
            runId,
            toolCallId: callId,
            closeOnSuspend: true,
            onFinish: () => {
              callbacks.push('finish');
              notified.release();
            },
            onError: () => {
              callbacks.push('error');
              notified.release();
            },
            onAbort: () => {
              callbacks.push('abort');
              notified.release();
            },
          };
          try {
            let resumed: any;
            if (mode === 'stale') {
              const first = await shared.agent.resumeStream({ approved: true, stage: 1 }, options);
              outputs.push(first);
              const firstChunks = await within(drain(first));
              expect(firstChunks.some((c: any) => c.type === 'tool-call-suspended')).toBe(true);
              expect((await host.waitForTerminal()).status).toBe('FAILED');
              await host.waitForLifecycleRequest();
              resumed = await shared.agent.resumeStream({ approved: true, stage: 2 }, options);
            } else resumed = await shared.agent.approveToolCall(options);
            outputs.push(resumed);
            await host.waitForRequest();
            await within(started.promise);
            if (mode === 'stale') {
              const consuming = drain(resumed);
              host.releaseLifecycle();
              await host.waitForLifecycleDelivery();
              await shared.agent.getPubSub()?.flush?.();
              expect(callbacks).toEqual([]);
              expect(probe(runId, callId)).toBe('held');
              await expect(shared.agent.approveToolCall({ runId, toolCallId: callId })).rejects.toMatchObject({
                id: 'AGENT_RESUME_ALREADY_CLAIMED',
              });
              held.release();
              const chunks = await within(consuming);
              expect(
                chunks
                  .filter((c: any) => c.type === 'text-delta')
                  .map((c: any) => c.payload.text)
                  .join(''),
              ).toBe('Done.Done.');
              expect(callbacks).toEqual(['finish']);
              expect(stages).toEqual([1, 2]);
            } else if (mode === 'cancel') {
              const observers = await Promise.all([shared.agent.observe(runId), shared.agent.observe(runId)]);
              observations.push(...observers);
              const consuming = observers.map(drain);
              // The owner does not consume its stream before this callback and claim measurement.
              await host.cancel();
              await host.waitForNotification();
              await within(notified.promise);
              expect(callbacks).toEqual(['abort']);
              expect(probe(runId, callId)).toBe('available');
              await expect(shared.agent.approveToolCall({ runId, toolCallId: callId })).rejects.toMatchObject({
                id: 'AGENT_RESUME_ABORTED_RUN',
              });
              expect(stages).toEqual([1]);
              held.release();
              await within(ended.promise);
              expect((await host.waitForTerminal()).status).toBe('CANCELLED');
              for (const task of consuming) {
                const chunks = await within(task);
                expect(chunks.filter((c: any) => c.type === 'abort')).toHaveLength(1);
              }
              await within(drain(resumed));
              const replay = await shared.agent.observe(runId);
              observations.push(replay);
              const replayChunks = await within(drain(replay));
              expect(replayChunks.filter((c: any) => c.type === 'abort')).toHaveLength(1);
            } else {
              const consuming = drain(resumed);
              await host.waitForHeldDelivery();
              expect((await host.waitForTerminal()).status).toBe('COMPLETED');
              expect(callbacks).toEqual([]);
              expect(probe(runId, callId)).toBe('held');
              await expect(shared.agent.approveToolCall({ runId, toolCallId: callId })).rejects.toMatchObject({
                id: 'AGENT_RESUME_ALREADY_CLAIMED',
              });
              host.releaseDelivery();
              const chunks = await within(consuming);
              expect(
                chunks
                  .filter((c: any) => c.type === 'text-delta')
                  .map((c: any) => c.payload.text)
                  .join(''),
              ).toBe('Done.Done.');
              expect(callbacks).toEqual(['finish']);
              expect(stages).toEqual([1]);
            }
            expect(probe(runId, callId)).toBe('available');
            console.log(
              'APPROVAL_DELIVERY_EVIDENCE',
              JSON.stringify({
                mode,
                runId,
                callId,
                stages,
                callbacks,
                claim: probe(runId, callId),
                native: host.snapshot(),
              }),
            );
          } finally {
            held.release();
            host.releaseLifecycle();
            host.releaseDelivery();
            for (const observer of observations) observer.detach();
            for (const output of outputs) output.cleanup?.();
            await Promise.all(tasks.map(task => within(task, 2_000).catch(() => {})));
            await host.cleanup();
          }
        }, 60_000);
      }
    }
  });
}
