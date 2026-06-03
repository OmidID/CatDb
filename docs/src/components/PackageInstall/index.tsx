import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export default function PackageInstall() {
  const {siteConfig} = useDocusaurusContext();
  const version = String(siteConfig.customFields?.catdbVersion ?? 'unknown');

  return (
    <pre>
      <code>{`dotnet add package CatDb --version ${version}`}</code>
    </pre>
  );
}
