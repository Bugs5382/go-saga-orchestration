# Documentation site

The go-saga-orchestration documentation site, built with [Docusaurus](https://docusaurus.io/).
Published to GitHub Pages at <https://bugs5382.github.io/go-saga-orchestration/>.

## Develop

```bash
npm install
npm start          # dev server at http://localhost:3000/go-saga-orchestration/
```

`npm start`/`npm run build` run a `gen` step first (`npm run gen`) which:

- generates the Go **API reference** from godoc into `docs/reference/` via
  [`gomarkdoc`](https://github.com/princjef/gomarkdoc) (`scripts/gen-api.sh`), and
- copies the repo `CHANGELOG.md` into `src/pages/changelog.md` (`scripts/sync-changelog.mjs`).

Both outputs are generated (gitignored) — never hand-edit `docs/reference/` or
`src/pages/changelog.md`.

## Build

```bash
npm run build      # static output in build/
```

## Publishing

Publishing is automated, not manual — do **not** run `npm run deploy`. The
`.github/workflows/docs-publish.yaml` workflow, on a published release, cuts a
versioned snapshot (`docusaurus docs:version <tag>`), commits it to `main`, and
deploys to GitHub Pages. See the repo's release flow.

## Theme

The site uses the the-rabbit-hole brand (dark, monochrome body accent, Oswald
headings; sky-blue is reserved for the header component). The shared theme will
move into `the-rabbit-hole-tech/docs-theme` and be consumed here.
