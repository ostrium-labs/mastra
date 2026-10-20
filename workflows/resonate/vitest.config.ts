import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // e2e: these tests run against a Resonate server (docker compose, see pretest).
    name: 'e2e:workflows/resonate',
    environment: 'node',
    include: ['src/**/*.test.ts'],
    pool: 'forks',
    testTimeout: 60000,
    hookTimeout: 60000,
    fileParallelism: false,
  },
});
