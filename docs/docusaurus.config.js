// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

const { env } = require('node:process');
const isBlog = env.GLUESQL_DOC_TYPE === 'blog';
const docsBaseUrl = env.GLUESQL_DOCS_BASE_URL ?? '/docs/dev/';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'GlueSQL',
  tagline: 'GlueSQL is quite sticky. It attaches to anywhere',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://gluesql.org',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: isBlog ? '/blog/' : docsBaseUrl,

  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
        pages: false,
        ...(
          isBlog ? {
            docs: false,
            blog: {
              routeBasePath: '/',
              blogTitle: 'GlueSQL Blog',
              blogDescription: 'GlueSQL Blog',
              postsPerPage: 'ALL',
              blogSidebarTitle: 'All posts',
              blogSidebarCount: 'ALL',
              showReadingTime: true,
            },
          } : {
            docs: {
              sidebarPath: require.resolve('./sidebars.js'),
              routeBasePath: '/',
            },
          }
        )
      }),
    ],
  ],

  themeConfig: {
    metadata: [{ name: 'algolia-site-verification', content: 'CC2C02E45AAD3CFE' }],
    colorMode: {
      disableSwitch: false,
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'GlueSQL',
      items: [
        ...(isBlog ? [
          {
            to: '/',
            label: 'Blog',
            position: 'left',
          },
          {
            href: 'https://gluesql.org/docs',
            label: 'Docs',
            position: 'right',
          },
        ] : [
          {
            to: 'getting-started/rust',
            position: 'left',
            label: 'Getting Started',
            activeBasePath: 'getting-started',
          },
          {
            to: 'sql-syntax/intro',
            position: 'left',
            label: 'SQL Syntax',
            activeBasePath: 'sql-syntax',
          },
          {
            to: 'ast-builder/intro',
            position: 'left',
            label: 'AST Builder',
            activeBasePath: 'ast-builder',
          },
          {
            to: 'storages/intro',
            position: 'left',
            label: 'Storages',
            activeBasePath: 'storages',
          },
          {
            href: 'https://gluesql.org/blog',
            label: 'Blog',
            position: 'right',
          },
        ]),
        {
          href: 'https://github.com/gluesql/gluesql',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    prism: {
      theme: lightCodeTheme,
      darkTheme: darkCodeTheme,
      additionalLanguages: ['rust', 'toml'],
    },
  },
};

if (isBlog) {
  config.themeConfig = {
    ...config.themeConfig,
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: isBlog ? [
            {
              label: 'Go to docs',
              href: 'https://gluesql.org/docs',
            },
          ] : [
            {
              label: 'Getting Started',
              to: '/getting-started/rust',
            },
            {
              label: 'SQL Syntax',
              to: '/sql-syntax/intro',
            },
            {
              label: 'AST Builder',
              to: '/ast-builder/intro',
            },
            {
              label: 'Storages',
              to: '/storages/intro',
            },
          ],
        },
        {
          title: 'Resources',
          items: [
            {
              label: 'Blog',
              href: 'https://gluesql.org/blog',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/gluesql/gluesql',
            },
            {
              label: 'Discord',
              href: 'https://discord.gg/C6TDEgzDzY',
            },
          ],
        },
        {
          title: 'Package',
          items: [
            {
              label: 'crates.io',
              href: 'https://crates.io/crates/gluesql',
            },
            {
              label: 'npm',
              href: 'https://www.npmjs.com/package/gluesql',
            },
          ],
        },
      ],
    },
  };
}

module.exports = config;
