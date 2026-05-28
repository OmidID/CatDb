import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import styles from './index.module.css';

const features = [
  {
    title: 'Ordered tables',
    text: 'Open typed tables, write records by key, and scan them in key order with forward and backward navigation.',
  },
  {
    title: 'Waterfall Tree engine',
    text: 'Writes are buffered and cascaded through a WTree, a write-optimized B-tree variant designed for random-key workloads.',
  },
  {
    title: 'Embedded or server',
    text: 'Use CatDb directly from a file, memory, stream, heap, or through the CatDb.Server HTTP and TCP layers.',
  },
];

export default function Home(): ReactNode {
  return (
    <Layout
      title="CatDb documentation"
      description="Documentation for CatDb, an embedded ordered key-value database for .NET.">
      <main>
        <section className={styles.hero}>
          <div className="container">
            <div className={styles.heroGrid}>
              <div>
                <Heading as="h1" className={styles.title}>
                  CatDb
                </Heading>
                <p className={styles.subtitle}>
                  A high-performance embedded ordered key-value database for .NET,
                  powered by a write-optimized Waterfall Tree.
                </p>
                <div className={styles.actions}>
                  <Link className="button button--primary button--lg" to="/docs/quick-start">
                    Quick start
                  </Link>
                  <Link className="button button--secondary button--lg" to="/docs/database-engine">
                    Explore the engine
                  </Link>
                </div>
              </div>
              <div className={styles.terminal} aria-label="CatDb code sample">
                <pre>
                  <code>{`using var engine = CatDb.Database.CatDb.FromFile("app.catdb");

var table = engine.OpenXTable<long, Tick>("ticks");
table[1] = new Tick("MSFT", DateTime.UtcNow, 410.5);

engine.Commit();`}</code>
                </pre>
              </div>
            </div>
          </div>
        </section>

        <section className={styles.features}>
          <div className="container">
            <div className={styles.featureGrid}>
              {features.map((feature) => (
                <article className={styles.feature} key={feature.title}>
                  <h2>{feature.title}</h2>
                  <p>{feature.text}</p>
                </article>
              ))}
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
