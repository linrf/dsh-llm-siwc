#!/usr/bin/env python3
"""Incremental update for an existing VM; credentials remain on that VM."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import tempfile
import time
import urllib.request

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--candidate', required=True)
p.add_argument('--sha256', required=True)
p.add_argument('--module-candidate', required=True)
p.add_argument('--module-sha256', required=True)
a = p.parse_args()
lock = open('/opt/siwc-gateway/.deploy.lock', 'a')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
candidate = Path(a.candidate)
payload = candidate.read_bytes()
if hashlib.sha256(payload).hexdigest() != a.sha256:
    raise SystemExit('candidate SHA-256 mismatch')
module_candidate = Path(a.module_candidate)
module_payload = module_candidate.read_bytes()
if hashlib.sha256(module_payload).hexdigest() != a.module_sha256:
    raise SystemExit('streaming module SHA-256 mismatch')
subprocess.run(['/usr/local/bin/node', '--input-type=module', '--check'], input=module_payload, check=True)
text = payload.decode()
version = re.search(r"const VERSION = '([^']+)'", text).group(1)
subprocess.run(['/usr/local/bin/node', '--input-type=module', '--check'], input=payload, check=True)

def get_health():
    return json.loads(urllib.request.urlopen('http://127.0.0.1:1457/healthz', timeout=3).read())

def health(expected):
    for _ in range(60):
        try:
            if get_health().get('version') == expected: return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError('gateway did not become healthy at expected version')

def quiet():
    data = urllib.request.urlopen('http://127.0.0.1:1457/metrics', timeout=5).read().decode()
    for key in ('inflight_requests', 'queued_requests', 'active_streams'):
        match = re.search(r'^siwc_gateway_' + key + r' (\d+)$', data, re.M)
        if not match or int(match.group(1)):
            raise RuntimeError('gateway has active requests; retry after they finish')

# These are snapshots, not an atomic admission drain. Use a maintenance window.
quiet()
old_version = get_health()['version']
source = Path('/opt/siwc-gateway/siwc-gateway.mjs')
module = Path('/opt/siwc-gateway/streaming-json.mjs')
env = Path('/etc/siwc-gateway.env')
caddy = Path('/etc/caddy/Caddyfile')
dropin = Path('/etc/systemd/system/siwc-gateway.service.d/large-requests.conf')
settings = {
    'MAX_RESPONSE_CONCURRENCY': '8', 'MAX_UPLOAD_CONCURRENCY': '2',
    'MAX_UPLOAD_QUEUE': '8', 'UPLOAD_QUEUE_WAIT_MS': '30000',
    'MAX_QUEUE': '8', 'QUEUE_WAIT_MS': '30000',
    'MAX_WIRE_BODY_BYTES': '67108864', 'MAX_DECODED_BODY_BYTES': '67108864',
    'MAX_SPOOL_BYTES': '537395200', 'MIN_SPOOL_FREE_BYTES': '134217728',
    'SPOOL_DIR': '/var/lib/siwc-gateway/requests',
    'BODY_IDLE_TIMEOUT_MS': '30000', 'BODY_TOTAL_TIMEOUT_MS': '300000',
    'ZSTD_WINDOW_LOG_MAX': '23', 'UPSTREAM_UPLOAD_TIMEOUT_MS': '300000',
    'UPSTREAM_HEADERS_TIMEOUT_MS': '180000',
    'UPSTREAM_ERROR_BODY_TIMEOUT_MS': '10000',
}
lines = [line for line in env.read_text().splitlines()
         if line.partition('=')[0] not in set(settings) | {'MAX_BODY_BYTES', 'BODY_TIMEOUT_MS', 'MAX_CONCURRENCY', 'NORMALIZE_TIMEOUT_MS', 'NORMALIZE_WAIT_MS', 'NORMALIZE_HEAP_MIB'}]
new_env = '\n'.join(lines + [key + '=' + value for key, value in settings.items()]) + '\n'
new_caddy = re.sub(r'^\s*flush_interval\s+-1\s*\n', '', caddy.read_text(), flags=re.M)
caddy_changed = new_caddy != caddy.read_text()

def atomic(path, data, mode, uid=0, gid=0):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.' + path.name + '.', dir=path.parent)
    tmp = Path(name)
    try:
        os.fchmod(fd, mode)
        os.fchown(fd, uid, gid)
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists(): tmp.unlink()

# Validate before changing live files. mkstemp starts at 0600, including env.
if caddy_changed:
    fd, name = tempfile.mkstemp(prefix='.Caddyfile.check.', dir=caddy.parent)
    try:
        with os.fdopen(fd, 'w') as stream: stream.write(new_caddy)
        subprocess.run(['/usr/local/bin/caddy', 'validate', '--config', name, '--adapter', 'caddyfile'], check=True)
    finally:
        Path(name).unlink(missing_ok=True)
quiet()
backup = Path('/opt/siwc-gateway/backups') / (time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()) + '-' + str(os.getpid()))
backup.mkdir(parents=True, mode=0o700)
os.chmod(backup, 0o700)
manifest = []
for i, path in enumerate([source, env, caddy, dropin, module]):
    existed = path.exists()
    item = {'path': str(path), 'existed': existed, 'backup': str(backup / str(i))}
    if existed:
        meta = path.stat()
        item.update(mode=stat.S_IMODE(meta.st_mode), uid=meta.st_uid, gid=meta.st_gid)
        shutil.copy2(path, backup / str(i))
        os.chmod(backup / str(i), 0o600)
    manifest.append(item)
(backup / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')

gateway_restart_attempted = False
caddy_restart_attempted = False
try:
    atomic(env, new_env.encode(), 0o600)
    atomic(dropin, b'[Service]\nLimitCORE=0\nKillMode=control-group\nMemorySwapMax=0\nOOMPolicy=continue\n', 0o644)
    atomic(module, module_payload, 0o644)
    atomic(source, payload, 0o644)
    if caddy_changed: atomic(caddy, new_caddy.encode(), stat.S_IMODE(caddy.stat().st_mode), caddy.stat().st_uid, caddy.stat().st_gid)
    if hashlib.sha256(source.read_bytes()).hexdigest() != a.sha256:
        raise RuntimeError('installed source SHA-256 mismatch')
    if hashlib.sha256(module.read_bytes()).hexdigest() != a.module_sha256:
        raise RuntimeError('installed module SHA-256 mismatch')
    quiet()
    subprocess.run(['systemctl', 'daemon-reload'], check=True)
    gateway_restart_attempted = True
    subprocess.run(['systemctl', 'restart', 'siwc-gateway'], check=True)
    health(version)
    if caddy_changed:
        caddy_restart_attempted = True
        subprocess.run(['systemctl', 'restart', 'caddy'], check=True)
except Exception as failure:
    errors = []
    for item in manifest:
        try:
            dest = Path(item['path'])
            if item['existed']: atomic(dest, Path(item['backup']).read_bytes(), item['mode'], item['uid'], item['gid'])
            else: dest.unlink(missing_ok=True)
        except Exception as error: errors.append('restore ' + item['path'] + ': ' + type(error).__name__)
    for command in ([['systemctl', 'daemon-reload']]
                    + ([['systemctl', 'restart', 'siwc-gateway']] if gateway_restart_attempted else [])
                    + ([['systemctl', 'restart', 'caddy']] if caddy_restart_attempted else [])):
        try: subprocess.run(command, check=True)
        except Exception as error: errors.append('rollback service: ' + type(error).__name__)
    try: health(old_version)
    except Exception as error: errors.append('rollback health: ' + type(error).__name__)
    if errors: raise RuntimeError('update failed; rollback needs inspection: ' + '; '.join(errors)) from failure
    raise
candidate.unlink(missing_ok=True)
module_candidate.unlink(missing_ok=True)
print(json.dumps({'version': version, 'sha256': a.sha256, 'streamingModuleSha256': a.module_sha256, 'backup': str(backup), 'caddyChanged': caddy_changed}))
