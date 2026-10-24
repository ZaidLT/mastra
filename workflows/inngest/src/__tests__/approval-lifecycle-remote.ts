import { fork, spawn } from 'node:child_process';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { ReadableStreamDefaultReader } from 'node:stream/web';
import { fileURLToPath } from 'node:url';
import { markResumeInFlight } from '../../../../packages/core/src/agent/durable/run-registry';
import { buildLifecyclePeer } from './fixtures/approval-lifecycle-peer';
import { ensureInngestCliBinary } from './inngest-cli';

/** The API caller and actual HTTP worker share storage and Inngest, but no registry or claim map. */
export async function remoteApprovalLifecycle(mode: 'cancel' | 'exhausted', modelUrl: string, port: number) {
  port += 100;
  const offset = port - 4100;
  const native = spawn(
    ensureInngestCliBinary(),
    [
      'dev',
      '-p',
      String(port),
      '--no-discovery',
      '--retry-interval=1',
      `--connect-gateway-port=${18289 + offset}`,
      `--connect-gateway-grpc-port=${50052 + offset * 3}`,
      `--connect-executor-grpc-port=${50053 + offset * 3}`,
    ],
    { stdio: 'ignore' },
  );
  const stopNative = () => {
    native.kill('SIGKILL');
  };
  process.once('exit', stopNative);
  let ready = false;
  for (let i = 0; i < 100; i++) {
    try {
      if ((await fetch(`http://127.0.0.1:${port}/dev`)).ok) {
        ready = true;
        break;
      }
    } catch {
      /* server starting */
    }
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  if (!ready) {
    native.kill('SIGKILL');
    throw new Error('Isolated Inngest host did not start');
  }
  const directory = await mkdtemp(join(tmpdir(), 'mastra-lifecycle-peer-'));
  const config = {
    id: `lifecycle-${crypto.randomUUID()}`,
    dbUrl: `file:${join(directory, 'store.db')}`,
    modelUrl,
    port,
    mode,
  };
  const peer = buildLifecyclePeer(config);
  const messages: any[] = [];
  const logs: string[] = [];
  const worker = fork(
    fileURLToPath(new URL('./fixtures/approval-lifecycle-peer.ts', import.meta.url)),
    ['approval-lifecycle-peer', JSON.stringify(config)],
    {
      execArgv: ['--import', import.meta.resolve('tsx')],
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
      env: { ...process.env, INNGEST_DEV: '1', INNGEST_BASE_URL: `http://127.0.0.1:${port}` },
    },
  );
  worker.on('message', message => messages.push(message));
  worker.stdout?.on('data', data => logs.push(data.toString()));
  worker.stderr?.on('data', data => logs.push(data.toString()));
  const until = async (read: () => any, label: string, timeout = 30000) => {
    const deadline = Date.now() + timeout;
    while (Date.now() < deadline) {
      const result = await read();
      if (result) return result;
      if (worker.exitCode !== null) throw new Error(`Remote worker exited: ${logs.join('').slice(-3000)}`);
      await new Promise(resolve => setTimeout(resolve, 50));
    }
    console.log('REMOTE_LIFECYCLE_DIAGNOSTIC', JSON.stringify({ mode, messages }));
    throw new Error(`Remote lifecycle deadline: ${label}; ${logs.join('').slice(-6000)}`);
  };
  const gql = async (query: string, variables: Record<string, unknown>) => {
    const response = await fetch(`http://127.0.0.1:${port}/v0/gql`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ query, variables }),
    });
    return (await response.json()).data;
  };
  const probe = (runId: string) => {
    const release = markResumeInFlight(runId, 'remote_call');
    if (!release) return 'held';
    release();
    return 'available';
  };
  const outputs: any[] = [];
  let reader: ReadableStreamDefaultReader<any> | undefined;
  const callbacks: string[] = [];
  let settled = false;
  try {
    await until(() => messages.find(m => m.type === 'ready'), 'HTTP worker ready');
    await until(async () => {
      const info = await (await fetch(`http://127.0.0.1:${port}/dev`)).json();
      return info.functions?.some((fn: any) => fn.slug === `${config.id}-workflow.inngest:durable-agentic-loop`);
    }, 'remote registration');
    const initial = await peer.agent.stream('Operate.', {
      closeOnSuspend: true,
      memory: { thread: crypto.randomUUID(), resource: config.id },
    });
    outputs.push(initial);
    let initiallySuspended = false;
    const initialDrain = (async () => {
      for await (const chunk of initial.fullStream) {
        /* drain until approval */
      }
      initiallySuspended = true;
    })();
    void initialDrain.catch(error => logs.push(String(error)));
    await until(() => initiallySuspended, 'initial approval suspension');
    const resumed = await peer.agent.approveToolCall({
      runId: initial.runId,
      toolCallId: 'remote_call',
      onFinish: () => {
        callbacks.push('finish');
      },
      onAbort: () => {
        callbacks.push('abort');
      },
      onError: () => {
        callbacks.push('error');
      },
    });
    outputs.push(resumed);
    reader = resumed.fullStream.getReader();
    const consuming = (async () => {
      try {
        while (!(await reader!.read()).done) {
          /* real remote chunks */
        }
      } finally {
        settled = true;
      }
    })();
    void consuming.catch(() => {});
    const execution = await until(() => messages.find(m => m.type === 'execution'), 'resumed native execution');
    const claimDuringExecution = probe(initial.runId);
    if (mode === 'cancel') {
      await until(() => messages.find(m => m.type === 'tool-started'), 'remote tool gate');
      await gql('mutation($id: ULID!) { cancelRun(runID: $id) { id status } }', { id: execution.runId });
    }
    const terminal = await until(async () => {
      const data = await gql('query($id: String!) { run(runID: $id) { id status } }', { id: execution.runId });
      return ['FAILED', 'CANCELLED'].includes(data?.run?.status) ? data.run : undefined;
    }, 'native termination');
    await until(() => settled, 'caller stream settled');
    const claimAfterTermination = probe(initial.runId);
    worker.send({ type: 'release' });
    if (mode === 'cancel') await until(() => messages.find(m => m.type === 'tool-ended'), 'remote step drained');
    return {
      mode,
      runId: initial.runId,
      callerPid: process.pid,
      workerPid: worker.pid,
      claimDuringExecution,
      claimAfterTermination,
      terminal,
      callbacks,
      settled,
      messages,
    };
  } finally {
    if (worker.connected) worker.send({ type: 'release' });
    for (const output of outputs) output.cleanup?.();
    await reader?.cancel().catch(() => {});
    worker.kill('SIGTERM');
    await new Promise(resolve => {
      if (worker.exitCode !== null || worker.signalCode !== null) return resolve(undefined);
      const timer = setTimeout(() => {
        worker.kill('SIGKILL');
        resolve(undefined);
      }, 3000);
      worker.once('exit', () => {
        clearTimeout(timer);
        resolve(undefined);
      });
    });
    process.removeListener('exit', stopNative);
    native.kill('SIGKILL');
    await peer.close();
    await rm(directory, { recursive: true, force: true });
  }
}
