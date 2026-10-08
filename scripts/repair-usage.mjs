#!/usr/bin/env node
/**
 * Repair stored session events whose `usage` block was written in the
 * provider's snake_case shape instead of the harness `TokenUsage` shape.
 *
 * Symptom this fixes: sessions fail to project with
 *
 *   {"path":["uncachedInputTokens"],"received":"NaN"},
 *   {"path":["outputTokens"],"received":"NaN"}
 *
 * which makes every new session fail to create.
 *
 *   node scripts/repair-usage.mjs --dry-run     # report only (default safe)
 *   node scripts/repair-usage.mjs --apply       # back up and rewrite
 *
 * Session logs are concatenated zstd frames, one or more JSON lines each;
 * the repair rewrites every line as its own frame, which is what the app
 * appends.
 */
import fs from 'node:fs'
import path from 'node:path'
import zlib from 'node:zlib'

const argv = process.argv.slice(2)
const apply = argv.includes('--apply')
const home = process.env.DSH_HOME ?? path.join(process.env.HOME ?? '', '.dsh')
const sessionsRoot = path.join(home, 'sessions')

const MAGIC = Buffer.from([0x28, 0xb5, 0x2f, 0xfd])

/** Byte offsets of every zstd frame in a concatenated log. */
function frameOffsets(buf) {
  const starts = []
  for (let i = 0; i + 4 <= buf.length; i++) {
    if (buf.compare(MAGIC, 0, 4, i, i + 4) === 0) starts.push(i)
  }
  return starts
}

/** Decompress every frame and return the concatenated text. */
function readAll(file) {
  const buf = fs.readFileSync(file)
  const starts = frameOffsets(buf)
  let text = ''
  for (let k = 0; k < starts.length; k++) {
    const end = k + 1 < starts.length ? starts[k + 1] : buf.length
    try {
      text += zlib.zstdDecompressSync(buf.subarray(starts[k], end)).toString('utf8')
    } catch {
      // A frame that will not decode is left as it was.
    }
  }
  return text
}

function walk(dir, out = []) {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name)
    if (entry.isDirectory()) walk(full, out)
    else if (entry.name.endsWith('.jsonl.zstd')) out.push(full)
  }
  return out
}

/** Convert one provider usage object into the harness TokenUsage shape. */
function fixUsage(raw) {
  if (raw === null || typeof raw !== 'object') return { value: raw, changed: false }
  const usage = raw
  if (!('input_tokens' in usage) && !('output_tokens' in usage)) {
    return { value: raw, changed: false }
  }
  const num = (value) => (typeof value === 'number' && Number.isFinite(value) ? value : 0)
  const inputDetails = usage.input_tokens_details ?? {}
  const outputDetails = usage.output_tokens_details ?? {}
  const cacheRead = num(inputDetails.cached_tokens)
  const cacheWrite = num(inputDetails.cache_write_tokens)
  const reasoning = num(outputDetails.reasoning_tokens)

  const fixed = {
    inputTokens: Math.max(0, num(usage.input_tokens) - cacheRead),
    outputTokens: num(usage.output_tokens),
  }
  const total = num(usage.total_tokens)
  if (total > 0) fixed.totalTokens = total
  if (cacheRead > 0) fixed.cacheReadTokens = cacheRead
  if (cacheWrite > 0) fixed.cacheWriteTokens = cacheWrite
  if (reasoning > 0) fixed.reasoningTokens = reasoning
  return { value: fixed, changed: true }
}

let scanned = 0
let repaired = 0

for (const file of walk(sessionsRoot)) {
  scanned++
  const text = readAll(file)
  if (!text.includes('"input_tokens"')) continue

  const lines = text.split('\n').filter((line) => line.trim() !== '')
  let changedHere = 0
  const rewritten = lines.map((line) => {
    let event
    try {
      event = JSON.parse(line)
    } catch {
      return line
    }
    const data = event?.data
    if (data === null || typeof data !== 'object' || !('usage' in data)) return line
    const { value, changed } = fixUsage(data.usage)
    if (!changed) return line
    data.usage = value
    changedHere++
    return JSON.stringify(event)
  })

  if (changedHere === 0) continue
  repaired++
  console.log(`${apply ? 'repairing' : 'would repair'}: ${file}`)
  console.log(`  lines ${lines.length}, usage blocks fixed ${changedHere}`)

  if (!apply) continue

  const backup = `${file}.bak-usage`
  if (!fs.existsSync(backup)) fs.copyFileSync(file, backup)

  // One frame per line, matching how the app appends events.
  const encoded = rewritten.map((line) => zlib.zstdCompressSync(Buffer.from(`${line}\n`, 'utf8')))
  fs.writeFileSync(file, Buffer.concat(encoded), { mode: 0o600 })
  console.log(`  backup: ${backup}`)
}

console.log()
console.log(`scanned ${scanned} session logs, ${repaired} needed repair`)
console.log(apply ? 'done — restart DeepSeek Harness' : 'dry run only; re-run with --apply to rewrite')
