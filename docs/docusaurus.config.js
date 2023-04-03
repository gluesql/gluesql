// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const lightCodeTheme = require('prism-react-renderer/themes/github');
const darkCodeTheme = require('prism-react-renderer/themes/dracula');

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'GlueSQL',
  tagline: 'GlueSQL is quite sticky. It attaches to anywhere',
  favicon: 'img/favicon.ico',

  // Set the production url of your site here
  url: 'https://gluesql.org',
  // Set the /<baseUrl>/ pathname under which your site is served
  // For GitHub pages deployment, it is often '/<projectName>/'
  baseUrl: '/',

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: 'facebook', // Usually your GitHub org/user name.
  projectName: 'docusaurus', // Usually your repo name.

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
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
        },
        blog: false,
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      }),
    ],
  ],

  themeConfig: {
    // Replace with your project's social card
    image: 'img/docusaurus-social-card.jpg',
    colorMode: {
      disableSwitch: true,
    },
    navbar: {
      title: 'GlueSQL',
      items: [
        {
          type: 'doc',
          docId: 'getting-started/rust/installation',
          position: 'left',
          label: 'Getting Started',
        },
        {
          type: 'doc',
          docId: 'sql-syntax/intro',
          position: 'left',
          label: 'SQL Syntax',
        },
        {
          type: 'doc',
          docId: 'ast-builder/intro',
          position: 'left',
          label: 'AST Builder',
        },
        {
          type: 'doc',
          docId: 'storages/intro',
          position: 'left',
          label: 'Storages',
        },
        {
          href: 'https://github.com/gluesql/gluesql',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {
              label: 'Getting Started',
              to: '/docs/getting-started/rust/installation',
            },
            {
              label: 'SQL Syntax',
              to: '/docs/sql-syntax/intro',
            },
            {
              label: 'AST Builder',
              to: '/docs/ast-builder/intro',
            },
            {
              label: 'Storages',
              to: '/docs/storages/intro',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/gluesql/gluesql',
            },
            {
              label: 'Discord',
              href: 'https://discord.gg/rRPb3Xqjmh',
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
    prism: {
      theme: lightCodeTheme,
      darkTheme: darkCodeTheme,
    },
  },
};

module.exports = config;
