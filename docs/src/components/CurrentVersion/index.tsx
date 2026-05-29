import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export default function CurrentVersion() {
  const {siteConfig} = useDocusaurusContext();
  return <>{String(siteConfig.customFields?.catdbVersion ?? 'unknown')}</>;
}
