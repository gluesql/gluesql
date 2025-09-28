import { defineConfig } from 'vite';
import { nodePolyfills } from 'vite-plugin-node-polyfills';

export default defineConfig({
  server: {
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
  plugins: [
    nodePolyfills({
      include: ['util'],
    }),
    {
      name: 'configure-response-headers',
      enforce: 'pre',
      configureServer: (server) => {
        server.middlewares.use((_req, res, next) => {
          res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp')
          res.setHeader('Cross-Origin-Opener-Policy', 'same-origin')
          next()
        })
      },
    },
  ],
  test: {
    include: ['tests/browser/**/*.{spec,test}.{js,ts}'],
    browser: {
      provider: 'playwright',
      enabled: true,
      instances: [
        {
          browser: 'chromium',
        }
      ]
    }
  }
})
