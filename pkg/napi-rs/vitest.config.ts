import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'happy-dom',
          root: 'tests/browser',
          environment: 'happy-dom',
          browser: {
            provider: 'playwright',
            enabled: true,
            instances: [{
              browser: 'chromium',
            }],
          },
        },
      },
      {
        test: {
          name: 'node',
          root: 'tests/node',
          environment: 'node',
        },
      },
    ],
  },
})
