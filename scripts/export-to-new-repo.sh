#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Export the promo-exchange site (web/ + .github/workflows/pages.yml) to a
# brand-new standalone git repository and push it to a remote of your choice.
#
#   usage:
#     ./scripts/export-to-new-repo.sh <remote-url> [target-dir]
#
#   example:
#     ./scripts/export-to-new-repo.sh git@github.com:max225777/dust-promo.git
#     ./scripts/export-to-new-repo.sh https://github.com/max225777/dust-promo.git ../dust-promo
#
# Steps it performs:
#   1. Creates <target-dir> (default: ../<repo-name-from-url>)
#   2. Copies web/* (incl. dotfiles) and the Pages workflow into it, with the
#      web/ contents flattened to the repo root
#   3. Rewrites .github/workflows/pages.yml so it triggers on `main` and on
#      the default branch rather than the Bk1RH feature branch
#   4. Runs `git init -b main`, initial commit, sets remote, pushes
#
# Requirements:
#   - git installed and authenticated for the target remote
#     (ssh key, gh auth, credential helper, or a PAT in the URL — whatever
#      you normally use)
# ---------------------------------------------------------------------------
set -euo pipefail

REMOTE_URL="${1:-}"
TARGET_DIR="${2:-}"

if [[ -z "$REMOTE_URL" ]]; then
  echo "usage: $0 <remote-url> [target-dir]" >&2
  exit 2
fi

SRC_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [[ ! -d "$SRC_ROOT/web/public" ]]; then
  echo "error: expected $SRC_ROOT/web/public to exist" >&2
  exit 1
fi

if [[ -z "$TARGET_DIR" ]]; then
  name="${REMOTE_URL##*/}"
  name="${name%.git}"
  TARGET_DIR="$(cd "$SRC_ROOT/.." && pwd)/${name:-promo-exchange}"
fi

if [[ -e "$TARGET_DIR" ]]; then
  echo "error: target already exists: $TARGET_DIR" >&2
  echo "pick a different path or remove it first." >&2
  exit 1
fi

echo "==> creating $TARGET_DIR"
mkdir -p "$TARGET_DIR"

echo "==> copying web/ contents"
# Copy regular + dotfiles, but skip node_modules and any local .env
(cd "$SRC_ROOT/web" && \
  tar --exclude='./node_modules' \
      --exclude='./.env' \
      --exclude='./.env.local' \
      --exclude='./npm-debug.log*' \
      -cf - . ) | (cd "$TARGET_DIR" && tar -xf -)

echo "==> copying Pages workflow"
mkdir -p "$TARGET_DIR/.github/workflows"
cp "$SRC_ROOT/.github/workflows/pages.yml" "$TARGET_DIR/.github/workflows/pages.yml"

echo "==> rewriting workflow path filters and branch filters for a standalone repo"
python3 - "$TARGET_DIR/.github/workflows/pages.yml" <<'PY'
import sys, re, pathlib
p = pathlib.Path(sys.argv[1])
src = p.read_text()
# In the standalone repo the site lives at the repo root, so paths change:
#   'web/public/**' -> 'public/**'
src = src.replace("'web/public/**'", "'public/**'")
# Upload path changes the same way:
src = src.replace("path: web/public", "path: public")
# Trigger only on main in the new repo (drop the feature-branch filter).
src = re.sub(
    r"branches:\n(?:\s+-\s+[^\n]+\n)+",
    "branches:\n      - main\n",
    src,
    count=1,
)
p.write_text(src)
PY

echo "==> writing .gitignore"
cat > "$TARGET_DIR/.gitignore" <<'GITIGNORE'
node_modules/
.env
.env.local
npm-debug.log*
.DS_Store
GITIGNORE

echo "==> git init + initial commit"
cd "$TARGET_DIR"
git init -b main >/dev/null
git add -A
git -c user.email="exporter@local" -c user.name="exporter" \
    commit -m "Initial import: promo exchange landing" >/dev/null

echo "==> adding remote and pushing"
git remote add origin "$REMOTE_URL"
git push -u origin main

echo
echo "✓ Done. Repo at: $TARGET_DIR"
echo "  Pushed to:     $REMOTE_URL"
echo
echo "Next steps on GitHub:"
echo "  1. Settings → Pages → Build and deployment → Source: GitHub Actions"
echo "  2. Actions → 'Deploy web/public to GitHub Pages' → Run workflow"
echo "     (or just push again to trigger it)"
