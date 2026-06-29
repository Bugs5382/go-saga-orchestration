#!/usr/bin/env node
// Generate an AI-agent-readable view of the docs site:
//   static/llms.txt       - an index (the llms.txt convention)
//   static/llms-full.txt  - every doc page concatenated as plain Markdown
// Both are generated build output (gitignored). Run after gen:api so the
// generated API reference under docs/reference/ is included.
import {readdirSync, readFileSync, writeFileSync, mkdirSync} from 'node:fs';
import {join, dirname, relative, sep} from 'node:path';
import {fileURLToPath} from 'node:url';

const WEBSITE = dirname(dirname(fileURLToPath(import.meta.url)));
const DOCS = join(WEBSITE, 'docs');
const STATIC = join(WEBSITE, 'static');

const SITE = 'https://bugs5382.github.io';
const BASE = '/go-saga-orchestration/';
const NAME = 'go-saga-orchestration';
const SUMMARY =
  'A standalone, solution-agnostic saga orchestrator and synchronous CEL rule ' +
  'evaluator. Embed it as a Go library or run it as two service binaries.';

// --- collect the Markdown pages -------------------------------------------
const files = readdirSync(DOCS, {recursive: true})
  .map(String)
  .filter((f) => f.endsWith('.md') || f.endsWith('.mdx'))
  .sort();

function parse(raw) {
  let body = raw;
  let fm = {};
  const m = raw.match(/^---\n([\s\S]*?)\n---\n?/);
  if (m) {
    body = raw.slice(m[0].length);
    for (const line of m[1].split('\n')) {
      const kv = line.match(/^([A-Za-z_]+):\s*(.*)$/);
      if (kv) fm[kv[1]] = kv[2].replace(/^['"]|['"]$/g, '').trim();
    }
  }
  return {fm, body: body.trim()};
}

const stripEmoji = (s) =>
  s.replace(/^[\s\u{1F000}-\u{1FFFF}\u{2190}-\u{27BF}\u{2B00}-\u{2BFF}️]+/u, '').trim();

function title(fm, body, rel) {
  if (fm.title) return fm.title;
  const h1 = body.match(/^#\s+(.+)$/m);
  if (h1) return stripEmoji(h1[1]);
  return rel.replace(/\.(md|mdx)$/, '');
}

function route(fm, rel) {
  if (fm.slug === '/') return BASE;
  const path = rel.replace(/\.(md|mdx)$/, '').split(sep).join('/');
  return `${BASE}docs/${path}`;
}

const pages = files.map((rel) => {
  const {fm, body} = parse(readFileSync(join(DOCS, rel), 'utf8'));
  return {
    rel,
    body,
    title: title(fm, body, rel),
    url: `${SITE}${route(fm, rel)}`,
    order: fm.sidebar_position ? Number(fm.sidebar_position) : 999,
  };
});
pages.sort((a, b) => a.order - b.order || a.rel.localeCompare(b.rel));

// --- llms.txt (index) ------------------------------------------------------
const index = [
  `# ${NAME}`,
  '',
  `> ${SUMMARY}`,
  '',
  `Full Markdown of every page: ${SITE}${BASE}llms-full.txt`,
  '',
  '## Docs',
  '',
  ...pages.map((p) => `- [${p.title}](${p.url})`),
  '',
].join('\n');

// --- llms-full.txt (everything) -------------------------------------------
const full = [
  `# ${NAME}`,
  '',
  `> ${SUMMARY}`,
  '',
  ...pages.flatMap((p) => [
    '',
    '---',
    '',
    `# ${p.title}`,
    `Source: ${p.url}`,
    '',
    p.body,
  ]),
  '',
].join('\n');

mkdirSync(STATIC, {recursive: true});
writeFileSync(join(STATIC, 'llms.txt'), index);
writeFileSync(join(STATIC, 'llms-full.txt'), full);

const rels = pages.map((p) => relative(WEBSITE, join(DOCS, p.rel))).join(', ');
console.log(`generated llms.txt + llms-full.txt from ${pages.length} pages (${rels})`);
