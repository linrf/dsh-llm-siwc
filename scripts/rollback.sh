#!/usr/bin/env bash
#
# EMERGENCY ROLLBACK for dsh-llm-siwc.
#
# Run this ONLY if DeepSeek Harness stops working after the plugin was
# installed (for example: cannot create or resume a session).
#
#   bash ~/GitHub/dsh-llm-siwc/scripts/rollback.sh
#
# It removes the plugin from the DSH desktop profile and restores nothing else.
# Safe to run more than once. Afterwards, quit and reopen the app (Cmd+Q).
#
set -euo pipefail

DSH_HOME_DIR="${DSH_HOME:-$HOME/.dsh}"
PROFILE="${1:-$DSH_HOME_DIR/profiles/desktop}"
MANIFEST="$PROFILE/package.json"
PLUGIN="dsh-llm-siwc"

if [ ! -f "$MANIFEST" ]; then
  echo "error: no profile manifest at $MANIFEST" >&2
  echo "       (pass the profile directory as the first argument if it differs)" >&2
  exit 1
fi

echo "profile : $PROFILE"
echo "plugin  : $PLUGIN"
echo

python3 - "$MANIFEST" "$PROFILE" "$PLUGIN" <<'PY'
import json, pathlib, shutil, sys

manifest = pathlib.Path(sys.argv[1])
profile = pathlib.Path(sys.argv[2])
plugin = sys.argv[3]

data = json.loads(manifest.read_text())

# Keep a copy before touching anything.
backup = manifest.with_name(manifest.name + '.bak-rollback')
shutil.copy2(manifest, backup)

before = len(data.get('dsh', {}).get('profile', {}).get('bundles', []))

data.get('dependencies', {}).pop(plugin, None)
section = data.setdefault('dsh', {}).setdefault('profile', {})
bundles = section.get('bundles', [])
section['bundles'] = [b for b in bundles if b != plugin]
manifest.write_text(json.dumps(data, indent=2) + '\n')

removed_link = False
link = profile / 'node_modules' / plugin
if link.is_symlink():
    link.unlink()
    removed_link = True
elif link.exists():
    shutil.rmtree(link)
    removed_link = True

print('backup written     :', backup)
print('bundles            : %d -> %d' % (before, len(section['bundles'])))
print('dependency removed : yes')
print('node_modules link  :', 'removed' if removed_link else 'not present')
print('plugin still listed:', plugin in section['bundles'])
PY

echo
echo "Rollback done. Now quit and reopen DeepSeek Harness (Cmd+Q, then open it)."
echo "Credentials under ~/.dsh/siwc are kept, so a later reinstall needs no new sign-in."
