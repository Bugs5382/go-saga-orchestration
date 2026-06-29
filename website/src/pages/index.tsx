import type {ReactNode} from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import CodeBlock from '@theme/CodeBlock';
import styles from './index.module.css';

const heroCode = `sc := saga.InMemory()

sc.RegisterVerb("charge_card", "common",
  verbs.HandlerFunc(func(ctx context.Context, run domain.SagaRun, step domain.Step) (map[string]any, error) {
    return map[string]any{"ok": true}, nil
  }))

sc.Register(domain.WorkflowDefinition{
  ID: "checkout", Version: 1, Start: "charge", Published: true,
  Steps: []domain.Step{
    {ID: "charge", Type: "charge_card", Next: "done"},
    {ID: "done", Type: domain.StepTypeEnd},
  },
})

runID, _ := sc.Start(ctx, "checkout", map[string]any{"total": 4200})
run, _ := sc.Get(ctx, runID)
// run.State == succeeded
`;

const features: {icon: string; title: string; body: string}[] = [
  {
    icon: '🧩',
    title: '31 saga step types',
    body: 'Transforms, HTTP/webhooks, timers, signals, events, parallel fan-out, foreach, loops, try/catch, human tasks, sub-sagas, and more.',
  },
  {
    icon: '📦',
    title: 'Embed or deploy',
    body: 'Run in-process with zero infrastructure, or deploy two Docker-friendly binaries backed by Postgres + RabbitMQ.',
  },
  {
    icon: '🧮',
    title: 'CEL expressions',
    body: 'Google CEL for conditions, transforms, filters, and routing — evaluated against live run variables.',
  },
  {
    icon: '⏰',
    title: 'Scheduled & event-driven',
    body: 'Cron-scheduled and event triggers, durable timers, and a license-gated feature model.',
  },
  {
    icon: '🧾',
    title: 'Durable audit trail',
    body: 'Every step transition, rule evaluation, signal, and metric is written as an immutable event row.',
  },
  {
    icon: '🔌',
    title: 'gRPC workers',
    body: 'Microservices connect over bidirectional gRPC streams to handle action steps without polling.',
  },
];

function Hero(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={styles.hero}>
      <div className="container">
        <Heading as="h1" className={styles.heroTitle}>
          {siteConfig.title}
        </Heading>
        <p className={styles.heroTagline}>{siteConfig.tagline}</p>
        <div className={styles.heroButtons}>
          <Link className="button button--secondary button--lg" to="/docs/getting-started">
            Get started →
          </Link>
          <Link className={`button button--lg ${styles.ghostButton}`} to="/docs">
            Documentation
          </Link>
          <Link
            className={`button button--lg ${styles.ghostButton}`}
            href="https://github.com/Bugs5382/go-saga-orchestration">
            GitHub
          </Link>
        </div>
      </div>
    </header>
  );
}

function Features(): ReactNode {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {features.map((f) => (
            <div className="col col--4" key={f.title}>
              <div className={styles.card}>
                <div className={styles.cardIcon} aria-hidden="true">
                  {f.icon}
                </div>
                <Heading as="h3" className={styles.cardTitle}>
                  {f.title}
                </Heading>
                <p>{f.body}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

function Quickstart(): ReactNode {
  return (
    <section className={styles.quickstart}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          30-second quickstart
        </Heading>
        <p className={styles.sectionLede}>
          Embed the engine in-process — no infrastructure — and run a saga:
        </p>
        <div className={styles.quickstartCode}>
          <CodeBlock language="go" title="checkout.go">
            {heroCode}
          </CodeBlock>
        </div>
        <div className={styles.quickstartLink}>
          <Link className="button button--primary button--lg" to="/docs/getting-started">
            Full tutorial →
          </Link>
        </div>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout title="Home" description={siteConfig.tagline as string}>
      <Hero />
      <main>
        <Features />
        <Quickstart />
      </main>
    </Layout>
  );
}
