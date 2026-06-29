import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'go-saga-orchestration',
  tagline: 'A standalone, solution-agnostic saga orchestrator + CEL rule evaluator',
  favicon: 'img/favicon.svg',

  url: 'https://bugs5382.github.io',
  baseUrl: '/go-saga-orchestration/',
  organizationName: 'Bugs5382',
  projectName: 'go-saga-orchestration',

  onBrokenLinks: 'warn',
  onBrokenMarkdownLinks: 'warn',

  i18n: {defaultLocale: 'en', locales: ['en']},

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          editUrl:
            'https://github.com/Bugs5382/go-saga-orchestration/tree/main/website/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: 'go-saga-orchestration',
      items: [
        {type: 'docSidebar', sidebarId: 'docsSidebar', position: 'left', label: 'Docs'},
        {to: '/changelog', label: 'Changelog', position: 'left'},
        {type: 'docsVersionDropdown', position: 'right'},
        {
          href: 'https://github.com/Bugs5382/go-saga-orchestration',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: '📚 Docs',
          items: [
            {label: 'Getting started', to: '/docs/getting-started'},
            {label: 'Architecture', to: '/docs/architecture'},
          ],
        },
        {
          title: '🔌 Reference',
          items: [
            {label: 'API reference', to: '/docs/reference/api'},
            {label: 'gRPC workers', to: '/docs/grpc'},
          ],
        },
        {
          title: '🔗 More',
          items: [
            {label: 'GitHub', href: 'https://github.com/Bugs5382/go-saga-orchestration'},
            {label: 'Changelog', to: '/changelog'},
          ],
        },
      ],
      copyright: `🐰 Copyright © ${new Date().getFullYear()} Shane — the rabbit hole`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['go', 'bash', 'json'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
