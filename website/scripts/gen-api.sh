#!/usr/bin/env bash
# Generate the Go API reference (gomarkdoc) into website/docs/reference/.
# Generated build output (gitignored); regenerated on every build/start.
set -euo pipefail

WEBSITE="$(cd "$(dirname "$0")/.." && pwd)"
ROOT="$(cd "$WEBSITE/.." && pwd)"
OUT="$WEBSITE/docs/reference"

if ! command -v gomarkdoc >/dev/null 2>&1; then
  go install github.com/princjef/gomarkdoc/cmd/gomarkdoc@latest
  export PATH="$PATH:$(go env GOPATH)/bin"
fi

rm -rf "$OUT"
mkdir -p "$OUT"

cat > "$OUT/_category_.json" <<'JSON'
{ "label": "API Reference", "position": 90, "link": { "type": "generated-index", "description": "Generated Go API reference for the public packages." } }
JSON

# Public packages (the library surface). clients/go/worker is a separate module
# and is documented in its own README.
cd "$ROOT"
gomarkdoc --output "$OUT/{{.Dir}}.md" \
  ./saga ./engine ./engine/verbs ./domain \
  ./store ./store/memory ./store/postgres ./store/redis \
  ./clock ./secrets ./api

# Strip gomarkdoc's per-package "## Index" block (a huge symbol list at the top
# of each page) — Docusaurus's own right-side TOC already provides navigation.
while IFS= read -r f; do
  awk '
    /^## Index$/ { skip=1; next }
    skip && /^## / { skip=0 }
    !skip { print }
  ' "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done < <(find "$OUT" -name '*.md')

echo "generated API reference -> $OUT"
