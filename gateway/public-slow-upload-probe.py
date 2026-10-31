#!/usr/bin/env python3
"""Synthetic 20MiB/40s curl upload; invalid tail prevents complete inference input."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

base = os.environ['SIWC_BASE_URL'].rstrip('/')
token = os.environ['SIWC_GATEWAY_TOKEN']
size = 20 * 1024 * 1024
prefix = b'{"model":"gpt-6.1-sol","instructions":"","input":[{"role":"user","content":[{"type":"input_text","text":"'
suffix = b'"}]}]}!'
with tempfile.TemporaryDirectory(prefix='siwc-public-upload-') as folder:
    body = Path(folder) / 'synthetic.json'
    response = Path(folder) / 'response.json'
    with body.open('wb') as out:
        out.write(prefix)
        chunk = b'x' * 65536
        remaining = size - len(prefix) - len(suffix)
        while remaining:
            part = chunk[:min(remaining, len(chunk))]
            out.write(part)
            remaining -= len(part)
        out.write(suffix)
    # Keep the bearer out of process arguments and files. curl reads config stdin.
    config = '\n'.join([
        'url = ' + json.dumps(base + '/responses'),
        'header = ' + json.dumps('Authorization: Bearer ' + token),
        'header = "Content-Type: application/json"',
    ]) + '\n'
    command = ['curl', '--config', '-', '--silent', '--show-error', '--connect-timeout', '15', '--max-time', '120', '--limit-rate', '512k', '--data-binary', '@' + str(body), '--output', str(response), '--write-out', '%{json}']
    if os.environ.get('SIWC_PROXY_URL'): command += ['--proxy', os.environ['SIWC_PROXY_URL']]
    started = time.monotonic()
    result = subprocess.run(command, input=config, capture_output=True, text=True)
    try: info = json.loads(result.stdout)
    except ValueError: info = {}
    try: reply = json.loads(response.read_text())
    except (ValueError, FileNotFoundError): reply = {}
    valid = result.returncode == 0 and info.get('http_code') == 400 and reply.get('error', {}).get('message') in ('invalid JSON body', 'invalid JSON body: trailing data') and info.get('size_upload') == size
    print(json.dumps({'client': 'curl', 'decodedMiB': 20, 'rateBytesPerSecond': 524288, 'exitCode': result.returncode, 'status': info.get('http_code'), 'uploadedBytes': info.get('size_upload'), 'seconds': round(time.monotonic() - started, 3), 'validationRejectedBeforeInference': valid}, indent=2))
    if not valid: raise SystemExit(1)
