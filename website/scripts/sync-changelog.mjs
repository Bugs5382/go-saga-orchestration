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

// The page is rendered as MDX, so a changelog entry that contains braces or a
// less-than (e.g. a PR title like "POST /sagas/{id}/cancel") would be parsed as
// a JS expression or a JSX tag and fail the build. Escape those to their HTML
// entities so the text renders literally. Release-drafter output is plain
// Markdown (headings, lists, bold, [text](url) links, @mentions, #refs, bare
// URLs), none of which use these characters, so escaping is safe.
const safeBody = body
  .replace(/</g, '&lt;')
  .replace(/\{/g, '&#123;')
  .replace(/\}/g, '&#125;');

const page = `---\ntitle: Changelog\n---\n\n${safeBody}\n`;
mkdirSync(pagesDir, {recursive: true});
writeFileSync(join(pagesDir, 'changelog.md'), page);
console.log('synced CHANGELOG.md -> src/pages/changelog.md');
