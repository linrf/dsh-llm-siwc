#!/usr/bin/env bash
# Usage: SIWC_SSH_TARGET=root@host [SIWC_SSH_IDENTITY=...] [SIWC_KNOWN_HOSTS=...]
#        [SIWC_CONTROL_PATH=...] bash gateway/deploy-update.sh
set -euo pipefail
: "${SIWC_SSH_TARGET:?set SIWC_SSH_TARGET to an existing gateway VM}"
gateway_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
ssh_args=(-F /dev/null -o BatchMode=yes -o ConnectTimeout=15 -o StrictHostKeyChecking=yes)
if [[ -n "${SIWC_SSH_IDENTITY:-}" ]]; then ssh_args+=(-i "$SIWC_SSH_IDENTITY" -o IdentitiesOnly=yes); fi
if [[ -n "${SIWC_KNOWN_HOSTS:-}" ]]; then ssh_args+=(-o "UserKnownHostsFile=$SIWC_KNOWN_HOSTS"); fi
if [[ -n "${SIWC_CONTROL_PATH:-}" ]]; then ssh_args+=(-S "$SIWC_CONTROL_PATH"); fi
source_sha=$(shasum -a 256 "$gateway_dir/siwc-gateway-v2.mjs" | cut -d ' ' -f 1)
candidate="/opt/siwc-gateway/candidate-${source_sha}-$$.mjs"
ssh "${ssh_args[@]}" "$SIWC_SSH_TARGET" "umask 077; cat > $candidate" < "$gateway_dir/siwc-gateway-v2.mjs"
ssh "${ssh_args[@]}" "$SIWC_SSH_TARGET" "python3 - --candidate $candidate --sha256 $source_sha" < "$gateway_dir/deploy-remote.py"
