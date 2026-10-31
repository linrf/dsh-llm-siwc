import { Transform } from 'node:stream'
import { TextDecoder } from 'node:util'

const stringSpecial = /["\\\x00-\x1f]/g
const whitespace = (c) => c === 0x20 || c === 0x09 || c === 0x0a || c === 0x0d
const digit = (c) => c >= 0x30 && c <= 0x39
const hex = (c) => digit(c) || (c >= 0x41 && c <= 0x46) || (c >= 0x61 && c <= 0x66)
const invalid = (message = 'invalid JSON body') => Object.assign(new Error(message), { status: 400, code: 'BODY_JSON' })
const limit = (message) => Object.assign(new Error(message), { status: 413, code: 'BODY_JSON_LIMIT' })

/**
 * Validates UTF-8 JSON without materializing values. Only top-level fields are
 * rewritten. Retained fields keep their lexical bytes (including item IDs,
 * escapes and number spelling); the root closing brace is withheld until EOF.
 * Memory is bounded by one input/output chunk, 256 key/model bytes and depth.
 */
export function createStreamingNormalizer({ unsupported = [], forbidden = [], maxBytes = Infinity, maxDepth = 256 } = {}) {
  return new StreamingNormalizer(unsupported, forbidden, maxBytes, maxDepth)
}

class StreamingNormalizer extends Transform {
  constructor(unsupported, forbidden, maxBytes, maxDepth) {
    super({ readableHighWaterMark: 64 * 1024, writableHighWaterMark: 64 * 1024 })
    this.omit = new Set([...unsupported, 'store', 'stream'])
    this.forbidden = new Set(forbidden)
    this.maxBytes = maxBytes
    this.maxDepth = maxDepth
    this.bytes = 0
    this.model = undefined
    this.decoder = new TextDecoder('utf-8', { fatal: true })
    this.stack = []
    this.started = false
    this.complete = false
    this.mode = null
    this.keep = false
    this.fields = 0
    this.keyParts = []
    this.keyLength = 0
    this.keyLong = false
    this.key = null
    this.capture = null
    this.captureLength = 0
  }

  _emit(data) {
    if (typeof data === 'string') data = Buffer.from(data)
    if (!data.length) return
    this.bytes += data.length
    if (this.bytes > this.maxBytes) throw limit('normalized request exceeds byte limit')
    this.parts.push(data)
  }

  _copy(data) {
    if (this.keep) this._emit(data)
    if (this.capture) {
      this.captureLength += data.length
      if (this.captureLength <= 256) this.capture.push(data)
      else this.capture = null
    }
  }

  _propertyPrefix() {
    if (this.fields++) this._emit(',')
  }

  _keyBytes(data) {
    if (this.keyLong) return this._emit(data)
    this.keyParts.push(data)
    this.keyLength += data.length
    if (this.keyLength > 256) {
      // No omitted field name can be this long. Stop retaining the key and
      // forward its raw bytes immediately, even when its closing quote is far away.
      this.keyLong = true
      this.keep = true
      this._propertyPrefix()
      for (const part of this.keyParts) this._emit(part)
      this.keyParts = []
    }
  }

  _raw(data) {
    if (this.stringRole === 'root-key') this._keyBytes(data)
    else this._copy(data)
  }

  _finishString() {
    this.mode = null
    if (this.stringRole === 'root-key') {
      if (this.keyLong) this.key = null
      else {
        const raw = Buffer.concat(this.keyParts, this.keyLength)
        this.key = JSON.parse(raw.toString('utf8'))
        if (this.forbidden.has(this.key)) throw invalid(this.key + ' is not supported by SIWC over HTTP; omit it and send the complete required history in input')
        this.keep = !this.omit.has(this.key)
        if (this.keep) { this._propertyPrefix(); this._emit(raw) }
      }
      this.keyParts = []
      this.stack.at(-1).state = 'colon'
    } else if (this.stringRole === 'key') this.stack.at(-1).state = 'colon'
    else this._valueDone()
    this.stringRole = null
  }

  _beginValue() {
    const context = this.stack.at(-1)
    if (!context || !['value', 'valueOrEnd'].includes(context.state)) throw invalid()
    context.state = 'inValue'
    if (context.root) {
      this.capture = this.key === 'model' ? [] : null
      this.captureLength = 0
    }
  }

  _valueDone() {
    const context = this.stack.at(-1)
    if (!context || context.state !== 'inValue') throw invalid()
    context.state = 'commaOrEnd'
    if (context.root) {
      if (this.capture) {
        const value = JSON.parse(Buffer.concat(this.capture, this.captureLength).toString('utf8'))
        this.model = typeof value === 'string' && /^[a-z0-9][a-z0-9_.:-]{0,127}$/i.test(value) ? value : undefined
      } else if (this.key === 'model') this.model = undefined
      this.capture = null
    }
  }

  _number(c) {
    const state = this.numberState
    if (state === 'minus') {
      if (!digit(c)) throw invalid()
      this.numberState = c === 0x30 ? 'zero' : 'int'
    } else if (state === 'zero' || state === 'int') {
      if (state === 'int' && digit(c)) return true
      if (c === 0x2e) this.numberState = 'dot'
      else if (c === 0x65 || c === 0x45) this.numberState = 'exp'
      else return false
    } else if (state === 'dot') {
      if (!digit(c)) throw invalid()
      this.numberState = 'fraction'
    } else if (state === 'fraction') {
      if (digit(c)) return true
      if (c === 0x65 || c === 0x45) this.numberState = 'exp'
      else return false
    } else if (state === 'exp') {
      if (c === 0x2b || c === 0x2d) this.numberState = 'expSign'
      else if (digit(c)) this.numberState = 'expDigits'
      else throw invalid()
    } else if (state === 'expSign') {
      if (!digit(c)) throw invalid()
      this.numberState = 'expDigits'
    } else if (state === 'expDigits') {
      if (!digit(c)) return false
    }
    return true
  }

  _finishNumber() {
    if (!['zero', 'int', 'fraction', 'expDigits'].includes(this.numberState)) throw invalid()
    this.mode = null
    this._valueDone()
  }

  _parse(chunk) {
    // Native regex scans long strings in chunks rather than allocating a JS
    // character/token per byte. Latin-1 preserves byte offsets into the Buffer.
    const text = chunk.toString('latin1')
    let at = 0
    while (at < chunk.length) {
      const c = chunk[at]
      if (this.mode === 'string') {
        if (this.unicodeDigits) {
          if (!hex(c)) throw invalid()
          this.unicodeDigits -= 1
          this._raw(chunk.subarray(at, ++at))
          continue
        }
        if (this.escaped) {
          if (c === 0x75) this.unicodeDigits = 4
          else if (![0x22, 0x5c, 0x2f, 0x62, 0x66, 0x6e, 0x72, 0x74].includes(c)) throw invalid()
          this.escaped = false
          this._raw(chunk.subarray(at, ++at))
          continue
        }
        stringSpecial.lastIndex = at
        const special = stringSpecial.exec(text)
        const end = special ? special.index : chunk.length
        if (end > at) this._raw(chunk.subarray(at, end))
        at = end
        if (at === chunk.length) continue
        const specialByte = chunk[at]
        if (specialByte < 0x20) throw invalid()
        this._raw(chunk.subarray(at, ++at))
        if (specialByte === 0x22) this._finishString()
        else this.escaped = true
        continue
      }
      if (this.mode === 'number') {
        const start = at
        while (at < chunk.length && this._number(chunk[at])) at += 1
        if (at > start) this._copy(chunk.subarray(start, at))
        if (at < chunk.length) this._finishNumber()
        continue
      }
      if (this.mode === 'literal') {
        if (c !== this.literal.charCodeAt(this.literalAt++)) throw invalid()
        this._copy(chunk.subarray(at, ++at))
        if (this.literalAt === this.literal.length) { this.mode = null; this._valueDone() }
        continue
      }
      if (whitespace(c)) {
        const start = at++
        while (at < chunk.length && whitespace(chunk[at])) at += 1
        if (this.stack.length > 1 || this.stack[0]?.state === 'inValue') this._copy(chunk.subarray(start, at))
        continue
      }
      if (!this.started) {
        if (c !== 0x7b) throw invalid('request body must be a JSON object')
        this.started = true
        this.stack.push({ type: 'object', state: 'keyOrEnd', root: true })
        this._emit('{')
        at += 1
        continue
      }
      if (this.complete) throw invalid('invalid JSON body: trailing data')
      const context = this.stack.at(-1)
      if (context.type === 'object' && ['keyOrEnd', 'key'].includes(context.state) && c === 0x22) {
        this.mode = 'string'
        this.stringRole = context.root ? 'root-key' : 'key'
        if (context.root) {
          this.keyParts = []; this.keyLength = 0; this.keyLong = false; this.key = null; this.keep = false
        }
        this._raw(chunk.subarray(at, ++at))
        continue
      }
      if (context.type === 'object' && c === 0x3a && context.state === 'colon') {
        context.state = 'value'
        this._copy(chunk.subarray(at, ++at))
        continue
      }
      if (c === 0x2c && context.state === 'commaOrEnd') {
        if (!context.root) this._copy(chunk.subarray(at, at + 1))
        context.state = context.type === 'object' ? 'key' : 'value'
        at += 1
        continue
      }
      if ((context.type === 'object' && c === 0x7d && ['keyOrEnd', 'commaOrEnd'].includes(context.state)) ||
          (context.type === 'array' && c === 0x5d && ['valueOrEnd', 'commaOrEnd'].includes(context.state))) {
        this.stack.pop()
        if (context.root) this.complete = true
        else { this._copy(chunk.subarray(at, at + 1)); this._valueDone() }
        at += 1
        continue
      }
      this._beginValue()
      if (c === 0x7b || c === 0x5b) {
        if (this.stack.length >= this.maxDepth) throw limit('JSON nesting exceeds processing limit')
        this._copy(chunk.subarray(at, ++at))
        this.stack.push({ type: c === 0x7b ? 'object' : 'array', state: c === 0x7b ? 'keyOrEnd' : 'valueOrEnd', root: false })
      } else if (c === 0x22) {
        this.mode = 'string'; this.stringRole = 'value'
        this._raw(chunk.subarray(at, ++at))
      } else if (c === 0x2d || digit(c)) {
        this.mode = 'number'
        this.numberState = c === 0x2d ? 'minus' : c === 0x30 ? 'zero' : 'int'
        this._copy(chunk.subarray(at, ++at))
      } else if (c === 0x74 || c === 0x66 || c === 0x6e) {
        this.mode = 'literal'; this.literal = c === 0x74 ? 'true' : c === 0x66 ? 'false' : 'null'; this.literalAt = 1
        this._copy(chunk.subarray(at, ++at))
      } else throw invalid()
    }
  }

  _transform(chunk, _encoding, callback) {
    this.parts = []
    try {
      this.decoder.decode(chunk, { stream: true })
      this._parse(chunk)
      const output = this.parts.length ? Buffer.concat(this.parts) : undefined
      this.parts = null
      callback(null, output)
    } catch (error) {
      this.parts = null
      callback(error.status ? error : invalid())
    }
  }

  _flush(callback) {
    this.parts = []
    try {
      this.decoder.decode()
      if (this.mode === 'number') this._finishNumber()
      if (!this.complete || this.mode || this.stack.length) throw invalid()
      this._emit((this.fields ? ',' : '') + '"store":false,"stream":true}')
      const output = Buffer.concat(this.parts)
      this.parts = null
      callback(null, output)
    } catch (error) {
      this.parts = null
      callback(error.status ? error : invalid())
    }
  }
}
