#!/usr/bin/env bash
# Install this plugin into a DSH profile.
#
# The Electron-managed `desktop` profile cannot be targeted by
# `dsh plugin --profile desktop`, so the equivalent edits are applied here:
# a dependency entry, a bundle-list entry, and a node_modules link.
set -euo pipefail

PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DSH_HOME="${DSH_HOME:-$HOME/.dsh}"
PROFILE="${1:-desktop}"
PROFILE_DIR="$DSH_HOME/profiles/$PROFILE"

[ -f "$PROFILE_DIR/package.json" ] || { echo "error: no profile at $PROFILE_DIR" >&2; exit 1; }
[ -f "$PLUGIN_DIR/lib/index.js" ] || { echo "error: build first (pnpm build)" >&2; exit 1; }

python3 - "$PROFILE_DIR" "$PLUGIN_DIR" <<'PY'
import json, sys, pathlib
profile_dir, plugin_dir = sys.argv[1], sys.argv[2]
p = pathlib.Path(profile_dir) / "package.json"
data = json.loads(p.read_text())
deps = data.setdefault("dependencies", {})
deps["dsh-llm-siwc"] = "link:%s" % plugin_dir
profile = data.setdefault("dsh", {}).setdefault("profile", {})
bundles = profile.setdefault("bundles", [])
if "dsh-llm-siwc" not in bundles:
    bundles.append("dsh-llm-siwc")
p.write_text(json.dumps(data, indent=2) + "\n")
print("profile updated:", p)
PY

mkdir -p "$PROFILE_DIR/node_modules/@deepseek-ai"
ln -sfn "$PLUGIN_DIR" "$PROFILE_DIR/node_modules/dsh-llm-siwc"

echo
echo "installed into profile '$PROFILE'."
echo "Restart the DeepSeek Harness app to load the plugin."
