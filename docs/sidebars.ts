import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  catdbSidebar: [
    'welcome',
    'quick-start',
    'setup-server',
    {
      type: 'category',
      label: 'Core Concepts',
      collapsed: false,
      items: [
        'database-engine',
        'table-and-xtable',
        'blob-storage',
        'index-and-search',
        'commit-and-transactions',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      collapsed: false,
      items: [
        'api-reference',
        'server-http-api',
        'architecture',
        'troubleshooting',
      ],
    },
  ],
};

export default sidebars;
