// Copy the repo CHANGELOG.md into the site as a global /changelog page.
// Generated build output (gitignored); regenerated on every build/start.
import {readFileSync, writeFileSync, mkdirSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {dirname, join} from 'node:path';

const here = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(here, '..', '..');
const pagesDir = join(here, '..', 'src', 'pages');

let body = '';
try {
  body = readFileSync(join(repoRoot, 'CHANGELOG.md'), 'utf8');
} catch {
  body = '# Changelog\n\nNo changelog yet.\n';
}

const page = `---\ntitle: Changelog\n---\n\n${body}\n`;
mkdirSync(pagesDir, {recursive: true});
writeFileSync(join(pagesDir, 'changelog.md'), page);
console.log('synced CHANGELOG.md -> src/pages/changelog.md');
