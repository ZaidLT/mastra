import fs from 'node:fs';
import path from 'node:path';
import { defineConfig } from 'vitest/config';
const core = path.resolve(import.meta.dirname, '../../core');
const pkg = JSON.parse(fs.readFileSync(path.join(core, 'package.json'), 'utf8'));
function coreSource(subpath: string) {
  const entry = pkg.exports[subpath ? `./${subpath}` : '.'];
  const dist = entry?.import?.default;
  const base = dist
    ? path.join(core, dist.replace(/^\.\/dist\//, 'src/').replace(/\.js$/, ''))
    : path.join(core, 'src', subpath);
  for (const candidate of [`${base}.ts`, path.join(base, 'index.ts')]) if (fs.existsSync(candidate)) return candidate;
  throw new Error(`Missing source: ${subpath}`);
}
export default defineConfig({
  define: { __MASTRA_VERSION__: JSON.stringify(pkg.version) },
  resolve: { alias: [{ find: /^@mastra\/core(?:\/(.*))?$/, replacement: '$1', customResolver: coreSource }] },
  root: import.meta.dirname,
  test: {
    include: ['src/durable-memory-reads.scenario.test.ts'],
    maxWorkers: 1,
    testTimeout: 60000,
    hookTimeout: 60000,
    retry: 0,
    ...(process.env.MEMORY_READ_INNGEST === '1'
      ? {
          setupFiles: [path.resolve(import.meta.dirname, '../../../workflows/inngest/src/__tests__/aimock/setup.ts')],
          globalSetup: [path.resolve(import.meta.dirname, '../../../workflows/inngest/src/__tests__/global-setup.ts')],
          env: { INNGEST_DEV: '1' },
        }
      : {}),
  },
});
