import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'CatDb',
  tagline: 'Embedded ordered key-value storage for .NET',
  favicon: 'img/catdb-icon.svg',

  future: {
    v4: true,
  },

  url: 'https://omidid.github.io',
  baseUrl: '/CatDb/',

  organizationName: 'OmidID',
  projectName: 'CatDb',

  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: 'docs',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/OmidID/CatDb/tree/master/docs/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/catdb-icon.svg',
    colorMode: {
      respectPrefersColorScheme: true,
    },
    navbar: {
      title: 'CatDb',
      logo: {
        alt: 'CatDb logo',
        src: 'img/catdb-icon.svg',
      },
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'catdbSidebar',
          position: 'left',
          label: 'Docs',
        },
        {
          href: 'https://github.com/OmidID/CatDb',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Use CatDb',
          items: [
            {label: 'Quick Start', to: '/docs/quick-start'},
            {label: 'Setup Server', to: '/docs/setup-server'},
            {label: 'API Reference', to: '/docs/api-reference'},
          ],
        },
        {
          title: 'Internals',
          items: [
            {label: 'Database Engine', to: '/docs/database-engine'},
            {label: 'Architecture', to: '/docs/architecture'},
            {label: 'Commit and Transactions', to: '/docs/commit-and-transactions'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'GitHub', href: 'https://github.com/OmidID/CatDb'},
            {label: 'NuGet', href: 'https://www.nuget.org/packages/CatDb/'},
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} CatDb contributors. Built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['csharp', 'bash', 'json'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
