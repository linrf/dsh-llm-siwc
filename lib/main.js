// src/main.ts
import z from "@deepseek-ai/schemastery";
import { LlmAdapter } from "@deepseek-ai/dsh-llm";
import { credentialKey } from "@deepseek-ai/dsh-credentials";
import { join as join3 } from "node:path";

// src/convert.ts
function textOf(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content.filter((part) => part.type === "text" && typeof part.text === "string").map((part) => part.text).join("");
}
function contentParts(content) {
  if (typeof content === "string") {
    return content === "" ? [] : [{ type: "input_text", text: content }];
  }
  if (!Array.isArray(content)) return [];
  const parts = [];
  for (const part of content) {
    if (part.type === "text" && typeof part.text === "string") {
      parts.push({ type: "input_text", text: part.text });
    } else if (part.type === "image" && part.image !== void 0) {
      const image = part.image;
      const url = typeof image.url === "string" ? image.url : void 0;
      const data = typeof image.data === "string" ? image.data : void 0;
      if (url) parts.push({ type: "input_image", image_url: url });
      else if (data) parts.push({ type: "input_image", image_url: data });
    }
  }
  return parts;
}
function convertMessages(messages, system) {
  const instructionParts = [];
  if (typeof system === "string" && system.trim() !== "") instructionParts.push(system);
  const input = [];
  for (const message of messages) {
    const role = message.role;
    if (role === "system" || role === "developer") {
      const text = textOf(message.content);
      if (text.trim() !== "") instructionParts.push(text);
      continue;
    }
    if (role === "tool") {
      input.push({
        type: "function_call_output",
        call_id: String(message.toolCallId ?? ""),
        output: typeof message.content === "string" ? message.content : textOf(message.content)
      });
      continue;
    }
    if (role === "assistant") {
      if (Array.isArray(message.toolCalls)) {
        for (const call of message.toolCalls) {
          input.push({
            type: "function_call",
            call_id: call.id,
            name: call.name,
            arguments: call.arguments || "{}"
          });
        }
      }
      const text = textOf(message.content);
      if (text.trim() !== "") {
        input.push({ role: "assistant", content: [{ type: "output_text", text }] });
      }
      continue;
    }
    const parts = contentParts(message.content);
    if (parts.length > 0) {
      input.push({ role: "user", content: parts });
    }
  }
  const instructions = instructionParts.join("\n\n");
  return instructions === "" ? { input } : { instructions, input };
}
function convertTools(tools) {
  if (!tools || tools.length === 0) return void 0;
  const converted = tools.filter((tool) => typeof tool.name === "string" && tool.name !== "").map((tool) => ({
    type: "function",
    name: tool.name,
    ...typeof tool.description === "string" && tool.description !== "" ? { description: tool.description } : {},
    parameters: tool.parameters ?? tool.inputSchema ?? { type: "object", properties: {} }
  }));
  return converted.length > 0 ? converted : void 0;
}

// src/config.ts
var DEFAULT_CALLBACK_HOST = "127.0.0.1";
var DEFAULT_CALLBACK_PORT = 1455;
var CALLBACK_PATH = "/auth/callback";
var AUTHORIZE_URL = "https://auth.openai.com/api/accounts/authorize";
var TOKEN_URL = "https://auth.openai.com/api/accounts/oauth/token";
var OIDC_DISCOVERY_URL = "https://auth.openai.com/.well-known/openid-configuration";
var JWKS_URL_FALLBACK = "https://auth.openai.com/.well-known/jwks.json";
var API_BASE_URL = "https://api.openai.com/v1";
var RESPONSES_PATH = "/responses";
var DYNAMIC_REGISTRATION_CLIENT_ID = "dynamic_agent_client";
var DEFAULT_AGENT_NAME = "DeepSeek Harness";
var REQUIRED_SCOPES = [
  "openid",
  "profile",
  "email",
  "offline_access",
  "resource.invoke",
  "chatgpt.tokens.use.direct"
];
var PLAN_USAGE_SCOPE = "chatgpt.tokens.use.direct";
function resolveConfig(partial = {}) {
  return {
    agentName: partial.agentName ?? DEFAULT_AGENT_NAME,
    callbackHost: partial.callbackHost ?? DEFAULT_CALLBACK_HOST,
    callbackPort: partial.callbackPort ?? DEFAULT_CALLBACK_PORT,
    storeDir: partial.storeDir ?? defaultStoreDir(),
    refreshLeadMs: partial.refreshLeadMs ?? 3e5,
    timeoutMs: partial.timeoutMs ?? 6e4
  };
}
function defaultStoreDir() {
  const home = process.env.DSH_HOME ?? `${process.env.HOME ?? ""}/.dsh`;
  return `${home}/siwc`;
}

// src/sse.ts
function isReadableStream(body) {
  return typeof body?.getReader === "function";
}
async function* parseSseStream(body) {
  const decoder = new TextDecoder();
  let buffer = "";
  const frames = function* (chunk) {
    buffer += chunk;
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      const trimmed = line.trimEnd();
      if (!trimmed.startsWith("data:")) continue;
      const payload = trimmed.slice(5).trim();
      if (payload === "" || payload === "[DONE]") continue;
      yield payload;
    }
  };
  const source = isReadableStream(body) ? (async function* () {
    const reader = body.getReader();
    try {
      for (; ; ) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value) yield value;
      }
    } finally {
      reader.releaseLock();
    }
  })() : body;
  for await (const chunk of source) {
    for (const payload of frames(decoder.decode(chunk, { stream: true }))) {
      try {
        const parsed = JSON.parse(payload);
        if (parsed && typeof parsed === "object") yield parsed;
      } catch {
      }
    }
  }
  for (const payload of frames("\n")) {
    try {
      const parsed = JSON.parse(payload);
      if (parsed && typeof parsed === "object") yield parsed;
    } catch {
    }
  }
}

// src/errors.ts
var USAGE_URL = "https://chatgpt.com/settings/usage";
var RESPONSES_CODES = {
  subscription_sharing_user_not_eligible: { action: "ineligible", status: 403 },
  subscription_sharing_usage_limit_exceeded: { action: "pause-account", status: 429 },
  subscription_sharing_usage_unavailable: { action: "retry-backoff", status: 503 },
  subscription_sharing_unsupported_capability: { action: "fix-request", status: 400 },
  subscription_sharing_route_not_supported: { action: "fix-request", status: 403 },
  subscription_sharing_invalid_user: { action: "reauth", status: 401 },
  chatpass_v2_scope_not_authorized: { action: "fix-client", status: 403 },
  chatpass_v2_invalid_authorization_context: { action: "fix-client", status: 403 },
  subscription_sharing_user_unavailable: { action: "retry-backoff", status: 503 }
};
var ADMISSION_STATUS = {
  401: "reauth",
  403: "ineligible",
  503: "retry-backoff"
};
function classifyError(input) {
  const base = {
    action: "fatal",
    status: input.status,
    message: `request failed with HTTP ${input.status}`,
    requestId: input.requestId
  };
  const structured = extractStructuredError(input.body);
  if (structured) {
    const known = structured.code ? RESPONSES_CODES[structured.code] : void 0;
    const action = known?.action ?? statusToAction(input.status);
    return {
      ...base,
      action,
      code: structured.code,
      param: structured.param,
      message: structured.message ?? base.message,
      usageUrl: structured.code === "subscription_sharing_usage_limit_exceeded" ? USAGE_URL : void 0
    };
  }
  const detail = extractDetail(input.body, input.rawBody);
  if (detail) {
    return {
      ...base,
      action: statusToAction(input.status),
      message: detail
    };
  }
  return { ...base, action: statusToAction(input.status) };
}
function statusToAction(status) {
  return ADMISSION_STATUS[status] ?? (status >= 500 ? "retry-backoff" : "fatal");
}
function extractStructuredError(body) {
  if (!body || typeof body !== "object") return null;
  const record = body;
  const error = record.error;
  if (error && typeof error === "object") {
    const e = error;
    const code = typeof e.code === "string" ? e.code : void 0;
    const param = typeof e.param === "string" ? e.param : void 0;
    const message = typeof e.message === "string" ? e.message : void 0;
    if (code || param || message) return { code, param, message };
  }
  const response = record.response;
  if (response && typeof response === "object") {
    const r = response;
    if (r.error) return extractStructuredError({ error: r.error });
  }
  return null;
}
function extractDetail(body, rawBody) {
  if (body && typeof body === "object") {
    const detail = body.detail;
    if (typeof detail === "string") return detail;
  }
  if (rawBody && rawBody.trim() && !rawBody.trimStart().startsWith("{")) {
    return rawBody.trim().slice(0, 500);
  }
  return null;
}
var UNSUPPORTED_RESPONSE_FIELDS = [
  "background",
  "conversation",
  "max_output_tokens",
  "max_tool_calls",
  "metadata",
  "moderation",
  "multi_agent",
  "prompt",
  "prompt_cache_retention",
  "safety_identifier",
  "temperature",
  "top_logprobs",
  "top_p",
  "truncation",
  "user"
];
function stripUnsupportedFields(body) {
  const out = { ...body };
  for (const field of UNSUPPORTED_RESPONSE_FIELDS) delete out[field];
  return out;
}

// src/client.ts
var ResponsesHttpError = class extends Error {
  status;
  body;
  requestId;
  constructor(status, body, requestId) {
    super(`responses request failed with HTTP ${status}`);
    this.name = "ResponsesHttpError";
    this.status = status;
    this.body = body;
    this.requestId = requestId;
  }
};
function requestBody(request) {
  const body = {
    model: request.model,
    input: request.input,
    // Mandatory for this flow.
    store: false,
    stream: true
  };
  if (request.instructions !== void 0 && request.instructions !== "") {
    body.instructions = request.instructions;
  }
  if (request.tools !== void 0 && request.tools.length > 0) {
    body.tools = request.tools;
  }
  return stripUnsupportedFields(body);
}
async function* streamResponses(request, options = {}) {
  const fetchImpl = options.fetchImpl ?? fetch;
  const url = `${options.baseUrl ?? API_BASE_URL}${RESPONSES_PATH}`;
  const response = await fetchImpl(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${request.apiKey ?? ""}`
    },
    body: JSON.stringify(requestBody(request)),
    signal: request.signal
  });
  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new ResponsesHttpError(
      response.status,
      body,
      response.headers.get("x-request-id") ?? void 0
    );
  }
  if (!response.body) {
    throw new ResponsesHttpError(response.status, "response carried no body");
  }
  for await (const event of parseSseStream(response.body)) {
    yield event;
  }
}

// src/adapter.ts
var SiwcResponsesAdapter = class {
  #options;
  constructor(options) {
    this.#options = options;
  }
  providerInfo(provider) {
    return { id: provider, name: "ChatGPT" };
  }
  /**
   * Advertised models for one route.
   *
   * `provider` is a REQUIRED field of `LlmModelInfo`; omitting it makes the
   * model directory reject the whole catalog with
   * "adapter returned invalid or duplicate model metadata".
   */
  listModels(provider) {
    return this.#options.models.map((id) => ({ provider, id, name: id }));
  }
  async resolveModel(provider, model) {
    return { provider, id: model, name: model };
  }
  /**
   * Stream one call, translating Responses SSE events to harness chunks.
   *
   * @param options - the assembled harness request.
   * @yields {StreamChunk} chunks in the order the harness expects.
   */
  async *stream(options) {
    const accessToken = await this.#options.resolveAccessToken(options.provider);
    const converted = convertMessages(options.messages, options.system);
    const tools = convertTools(options.tools);
    let index = 0;
    const toolBlocks = /* @__PURE__ */ new Map();
    const textBlocks = /* @__PURE__ */ new Set();
    let finished = false;
    const events = streamResponses(
      {
        model: options.model,
        apiKey: accessToken,
        instructions: converted.instructions,
        input: converted.input,
        tools,
        signal: options.signal
      },
      { baseUrl: this.#options.baseUrl, fetchImpl: this.#options.fetchImpl }
    );
    try {
      for await (const event of events) {
        for (const chunk of this.#translate(event, {
          textBlocks,
          toolBlocks,
          nextIndex: () => index++
        })) {
          if (chunk.type === "finish") finished = true;
          yield chunk;
        }
      }
    } catch (error) {
      if (error instanceof ResponsesHttpError) {
        const classified = classifyError({
          status: error.status,
          body: safeJson(error.body),
          rawBody: error.body,
          requestId: error.requestId
        });
        throw new ChatGptPlanError(classified);
      }
      throw error;
    }
    if (!finished) {
      throw new Error("responses stream ended without a terminal event");
    }
  }
  /** Translate one Responses event into zero or more harness chunks. */
  *#translate(event, state) {
    switch (event.type) {
      case "response.output_text.delta": {
        const text = typeof event.delta === "string" ? event.delta : "";
        if (text === "") return;
        const itemIndex = typeof event.output_index === "number" ? event.output_index : 0;
        if (!state.textBlocks.has(itemIndex)) {
          state.textBlocks.add(itemIndex);
          yield { type: "block-start", index: itemIndex, blockType: "text" };
        }
        yield { type: "text-delta", index: itemIndex, text };
        return;
      }
      case "response.reasoning_summary_text.delta":
      case "response.reasoning_text.delta": {
        const text = typeof event.delta === "string" ? event.delta : "";
        if (text === "") return;
        yield { type: "reasoning-delta", index: this.#reasoningIndex(state), text };
        return;
      }
      case "response.output_item.added": {
        const item = event.item;
        if (item?.type !== "function_call") return;
        const callId = String(item.call_id ?? item.id ?? "");
        const name2 = typeof item.name === "string" ? item.name : "";
        const blockIndex = state.nextIndex();
        const block = { id: callId, name: name2, arguments: "" };
        state.toolBlocks.set(callId, { index: blockIndex, block });
        yield { type: "block-start", index: blockIndex, blockType: "tool-call" };
        yield {
          type: "tool-call-delta",
          index: blockIndex,
          id: callId,
          name: name2,
          argumentsDelta: ""
        };
        return;
      }
      case "response.function_call_arguments.delta": {
        const callId = typeof event.item_id === "string" ? event.item_id : "";
        const entry = state.toolBlocks.get(callId);
        if (!entry) return;
        const delta = typeof event.delta === "string" ? event.delta : "";
        entry.block.arguments += delta;
        if (delta !== "") {
          yield {
            type: "tool-call-delta",
            index: entry.index,
            id: entry.block.id,
            argumentsDelta: delta
          };
        }
        return;
      }
      case "response.output_item.done": {
        const item = event.item;
        if (item?.type !== "function_call") return;
        const callId = String(item.call_id ?? item.id ?? "");
        const entry = state.toolBlocks.get(callId);
        if (!entry) return;
        const finalArguments = typeof item.arguments === "string" && item.arguments !== "" ? item.arguments : entry.block.arguments;
        yield {
          type: "block-end",
          index: entry.index,
          block: {
            type: "tool-call",
            id: entry.block.id,
            name: typeof item.name === "string" ? item.name : entry.block.name,
            arguments: finalArguments
          }
        };
        state.toolBlocks.delete(callId);
        return;
      }
      case "response.output_text.done": {
        const itemIndex = typeof event.output_index === "number" ? event.output_index : 0;
        if (!state.textBlocks.has(itemIndex)) return;
        const text = typeof event.text === "string" ? event.text : "";
        yield { type: "block-end", index: itemIndex, block: { type: "text", text } };
        state.textBlocks.delete(itemIndex);
        return;
      }
      case "response.completed": {
        const response = event.response;
        const usage = response?.usage;
        if (usage !== void 0) yield { type: "usage", usage };
        yield { type: "finish", reason: this.#finishReason(response) };
        return;
      }
      case "response.failed":
      case "response.incomplete": {
        const response = event.response;
        const error = response?.error ?? event;
        const classified = classifyError({ status: 200, body: { error } });
        throw new ChatGptPlanError(classified);
      }
      case "error": {
        const classified = classifyError({ status: 200, body: event });
        throw new ChatGptPlanError(classified);
      }
      default:
        return;
    }
  }
  #reasoningIndex(state) {
    return state.textBlocks.size === 0 ? 0 : -1;
  }
  #finishReason(response) {
    const status = typeof response?.status === "string" ? response.status : "";
    const reasons = response?.incomplete_details;
    const reason = typeof reasons?.reason === "string" ? reasons.reason : void 0;
    if (status === "incomplete" || reason === "max_output_tokens") return "length";
    return "stop";
  }
};
var ChatGptPlanError = class extends Error {
  classified;
  constructor(classified) {
    super(classified.message);
    this.name = "ChatGptPlanError";
    this.classified = classified;
  }
};
function safeJson(text) {
  try {
    return JSON.parse(text);
  } catch {
    return void 0;
  }
}

// src/host-id.ts
import { createHash as createHash2, generateKeyPairSync, randomUUID } from "node:crypto";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";

// src/crypto.ts
import { createHash, randomBytes } from "node:crypto";
function base64url(input) {
  return Buffer.from(input).toString("base64").replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
function randomToken(bytes = 32) {
  return base64url(randomBytes(bytes));
}
function createPkce() {
  const verifier = randomToken(32);
  const challenge = base64url(createHash("sha256").update(verifier).digest());
  return { verifier, challenge };
}
function decodeJwtPayload(token) {
  const parts = token.split(".");
  if (parts.length !== 3) throw new Error("malformed JWT: expected 3 segments");
  try {
    return JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
  } catch (error) {
    throw new Error(`malformed JWT payload: ${String(error)}`);
  }
}

// src/host-id.ts
var UUID_PREFIX = "urn:uuid:";
var THUMBPRINT_PREFIX = "urn:ietf:params:oauth:jwk-thumbprint:";
function generateUuidHostId() {
  return `${UUID_PREFIX}${randomUUID()}`;
}
function isValidHostId(value) {
  return value.startsWith(UUID_PREFIX) || value.startsWith(THUMBPRINT_PREFIX) || value.startsWith("did:key:");
}
async function loadOrCreateHostId(filePath, generate = generateUuidHostId) {
  try {
    const existing = (await readFile(filePath, "utf8")).trim();
    if (existing) {
      if (!isValidHostId(existing)) {
        throw new Error(`persisted host id has an unsupported format: ${existing}`);
      }
      return existing;
    }
  } catch (error) {
    const code = error.code;
    if (code !== "ENOENT") throw error;
  }
  const created = generate();
  await mkdir(dirname(filePath), { recursive: true, mode: 448 });
  await writeFile(filePath, created, { mode: 384 });
  return created;
}

// src/callback.ts
import { createServer } from "node:http";
var SUCCESS_HTML = `<!doctype html><meta charset="utf-8">
<title>Sign in complete</title>
<body style="font:16px/1.5 system-ui;padding:2rem">
<h2>Sign-in complete</h2><p>You can close this tab and return to your app.</p></body>`;
var FAILURE_HTML = `<!doctype html><meta charset="utf-8">
<title>Sign in failed</title>
<body style="font:16px/1.5 system-ui;padding:2rem">
<h2>Sign-in failed</h2><p>Return to your app for details.</p></body>`;
function listen(server, port, host) {
  return new Promise((resolve, reject) => {
    const onError = (error) => {
      server.removeListener("listening", onListening);
      reject(error);
    };
    const onListening = () => {
      server.removeListener("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(port, host);
  });
}
async function startCallbackServer(options) {
  const attempts = Math.max(1, options.portAttempts ?? 10);
  let server = null;
  let boundPort = 0;
  for (let i = 0; i < attempts; i++) {
    const candidate = options.port + i;
    const s = createServer();
    try {
      await listen(s, candidate, options.host);
      server = s;
      boundPort = candidate;
      break;
    } catch (error) {
      const code = error.code;
      s.close();
      if (code !== "EADDRINUSE" && code !== "EACCES") throw error;
    }
  }
  if (!server) {
    throw new Error(
      `could not bind a callback port in ${options.port}..${options.port + attempts - 1}`
    );
  }
  let settle;
  let fail;
  const waitForResult = new Promise((resolve, reject) => {
    settle = resolve;
    fail = reject;
  });
  let done = false;
  const cleanup = () => {
    if (done) return;
    done = true;
    clearTimeout(timer);
    options.signal?.removeEventListener("abort", onAbort);
    server?.close();
  };
  const timer = setTimeout(() => {
    cleanup();
    fail(new Error("timed out waiting for the authorization callback"));
  }, options.timeoutMs ?? 9e5);
  const onAbort = () => {
    cleanup();
    fail(new Error("authorization was cancelled"));
  };
  options.signal?.addEventListener("abort", onAbort, { once: true });
  server.on("request", (req, res) => {
    const url = new URL(req.url ?? "/", `http://${options.host}:${boundPort}`);
    if (url.pathname !== CALLBACK_PATH) {
      res.writeHead(404, { "content-type": "text/plain" });
      res.end("not found");
      return;
    }
    const params = url.searchParams;
    const error = params.get("error");
    if (error) {
      res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
      res.end(FAILURE_HTML);
      cleanup();
      fail(
        new Error(
          `authorization denied: ${error}${params.get("error_description") ? ` (${params.get("error_description")})` : ""}`
        )
      );
      return;
    }
    const code = params.get("code");
    const state = params.get("state");
    if (!code || !state) {
      res.writeHead(400, { "content-type": "text/plain" });
      res.end("missing code or state");
      return;
    }
    res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    res.end(SUCCESS_HTML);
    cleanup();
    settle({
      code,
      state,
      clientId: params.get("client_id") ?? void 0,
      scope: params.get("scope") ?? void 0
    });
  });
  return {
    redirectUri: `http://${options.host}:${boundPort}${CALLBACK_PATH}`,
    waitForResult,
    close: cleanup
  };
}

// src/oauth.ts
function buildAuthorizeUrl(params) {
  const isRegistration = params.clientId === void 0;
  const url = new URL(AUTHORIZE_URL);
  const set = (key, value) => {
    if (value !== void 0) url.searchParams.set(key, value);
  };
  set("client_id", isRegistration ? DYNAMIC_REGISTRATION_CLIENT_ID : params.clientId);
  if (isRegistration) set("agent_name_hint", params.agentNameHint);
  set("ext_agent_host_id", params.extAgentHostId);
  set("id_token_hint", params.idTokenHint);
  set("login_hint", params.loginHint);
  set("response_type", "code");
  set("redirect_uri", params.redirectUri);
  set("scope", (params.scope ?? REQUIRED_SCOPES).join(" "));
  set("resource", params.resource ?? API_BASE_URL);
  set("state", params.state);
  set("nonce", params.nonce);
  set("code_challenge_method", "S256");
  set("code_challenge", params.codeChallenge);
  return url.toString();
}
async function timedFetch(input, init, options) {
  const fetchImpl = options.fetchImpl ?? fetch;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), options.timeoutMs ?? 6e4);
  try {
    return await fetchImpl(input, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}
async function exchangeCode(params, options = {}) {
  const body = new URLSearchParams({
    grant_type: "authorization_code",
    client_id: params.clientId,
    code: params.code,
    code_verifier: params.codeVerifier,
    redirect_uri: params.redirectUri,
    resource: params.resource ?? API_BASE_URL
  });
  const res = await timedFetch(
    TOKEN_URL,
    {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body
    },
    options
  );
  const text = await res.text();
  if (!res.ok) {
    throw new OAuthError(`token exchange failed: HTTP ${res.status}`, text, res.status);
  }
  return parseTokenResponse(text);
}
async function refreshToken(params, options = {}) {
  const body = new URLSearchParams({
    grant_type: "refresh_token",
    client_id: params.clientId,
    refresh_token: params.refreshToken,
    resource: params.resource ?? API_BASE_URL
  });
  const res = await timedFetch(
    TOKEN_URL,
    {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body
    },
    options
  );
  const text = await res.text();
  if (!res.ok) {
    const code = extractErrorCode(text);
    throw new OAuthError(`token refresh failed: HTTP ${res.status}`, text, res.status, code);
  }
  return parseTokenResponse(text);
}
var OAuthError = class extends Error {
  body;
  status;
  code;
  constructor(message, body, status, code) {
    super(message);
    this.name = "OAuthError";
    this.body = body;
    this.status = status;
    this.code = code;
  }
};
function parseTokenResponse(text) {
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw new OAuthError("token response was not JSON", text, 0);
  }
  const response = parsed;
  if (!response.access_token) {
    throw new OAuthError("token response has no access_token", text, 0);
  }
  return response;
}
function extractErrorCode(body) {
  try {
    const parsed = JSON.parse(body);
    const error = parsed.error;
    if (typeof error === "string") return error;
    if (error && typeof error === "object" && "code" in error) {
      return String(error.code);
    }
  } catch {
  }
  return void 0;
}
var UNUSABLE_REFRESH_CODES = /* @__PURE__ */ new Set([
  "invalid_grant",
  "invalid_refresh_token",
  "token_expired",
  "refresh_token_expired",
  "refresh_token_invalidated",
  "refresh_token_reused"
]);
function isUnusableRefreshError(error) {
  return error instanceof OAuthError && error.code !== void 0 && UNUSABLE_REFRESH_CODES.has(error.code);
}

// src/verify.ts
import { createPublicKey, verify as cryptoVerify } from "node:crypto";
var ISSUER = "https://auth.openai.com";
var JwksCache = class {
  #keys = [];
  #fetchedAt = 0;
  #ttlMs = 36e5;
  #inflight = null;
  #fetchImpl;
  constructor(fetchImpl) {
    this.#fetchImpl = fetchImpl;
  }
  async keys(forceRefresh = false) {
    if (!forceRefresh && this.#keys.length > 0 && Date.now() - this.#fetchedAt < this.#ttlMs) {
      return this.#keys;
    }
    if (this.#inflight) return this.#inflight;
    this.#inflight = this.#load().finally(() => {
      this.#inflight = null;
    });
    return this.#inflight;
  }
  async #load() {
    let uri = JWKS_URL_FALLBACK;
    try {
      const res2 = await this.#fetchImpl(OIDC_DISCOVERY_URL);
      if (res2.ok) {
        const doc = await res2.json();
        if (doc.jwks_uri) uri = doc.jwks_uri;
      }
    } catch {
    }
    const res = await this.#fetchImpl(uri);
    if (!res.ok) throw new Error(`JWKS fetch failed: HTTP ${res.status}`);
    const body = await res.json();
    this.#keys = body.keys ?? [];
    this.#fetchedAt = Date.now();
    return this.#keys;
  }
};
var jwksCaches = /* @__PURE__ */ new WeakMap();
function cacheFor(fetchImpl) {
  let cache = jwksCaches.get(fetchImpl);
  if (!cache) {
    cache = new JwksCache(fetchImpl);
    jwksCaches.set(fetchImpl, cache);
  }
  return cache;
}
function jwkToKey(jwk) {
  const key = createPublicKey({ key: jwk, format: "jwk" });
  return key;
}
function joseToDer(signature) {
  const size = 32;
  if (signature.length !== size * 2) return signature;
  const r = signature.subarray(0, size);
  const s = signature.subarray(size);
  const encode = (part) => {
    let i = 0;
    while (i < part.length - 1 && part[i] === 0) i++;
    let v = part.subarray(i);
    if (v[0] & 128) v = Buffer.concat([Buffer.from([0]), v]);
    return v;
  };
  const rEnc = encode(r);
  const sEnc = encode(s);
  const len = 2 + rEnc.length + 2 + sEnc.length;
  return Buffer.concat([
    Buffer.from([48, len]),
    Buffer.from([2, rEnc.length]),
    rEnc,
    Buffer.from([2, sEnc.length]),
    sEnc
  ]);
}
function verifySignature(token, jwk) {
  const [headerB64, payloadB64, signatureB64] = token.split(".");
  const signed = Buffer.from(`${headerB64}.${payloadB64}`, "ascii");
  const signature = Buffer.from(signatureB64, "base64url");
  const key = jwkToKey(jwk);
  if (jwk.kty === "RSA") {
    return cryptoVerify("sha256", signed, key, signature);
  }
  if (jwk.kty === "EC") {
    if (cryptoVerify("sha256", signed, { key, dsaEncoding: "ieee-p1363" }, signature)) return true;
    return cryptoVerify("sha256", signed, key, joseToDer(signature));
  }
  if (jwk.kty === "OKP") {
    return cryptoVerify(null, signed, key, signature);
  }
  throw new Error(`unsupported JWK key type: ${jwk.kty}`);
}
async function verifyIdToken(token, options) {
  const fetchImpl = options.fetchImpl ?? fetch;
  const now = options.now ?? (() => Date.now());
  const tolerance = options.clockToleranceSec ?? 60;
  const [headerB64] = token.split(".");
  const header = JSON.parse(Buffer.from(headerB64, "base64url").toString("utf8"));
  const claims = decodeJwtPayload(token);
  const cache = cacheFor(fetchImpl);
  const verifyWith = async (forceRefresh) => {
    const keys = await cache.keys(forceRefresh);
    const candidates = header.kid ? keys.filter((k) => k.kid === header.kid) : keys.filter((k) => !header.alg || !k.alg || k.alg === header.alg);
    for (const jwk of candidates) {
      try {
        if (verifySignature(token, jwk)) return true;
      } catch {
      }
    }
    return false;
  };
  let signatureOk = await verifyWith(false);
  if (!signatureOk) signatureOk = await verifyWith(true);
  if (!signatureOk) throw new Error("ID token signature verification failed");
  if (claims.iss !== ISSUER) {
    throw new Error(`ID token issuer mismatch: expected ${ISSUER}, got ${claims.iss}`);
  }
  const audiences = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
  if (!audiences.includes(options.clientId)) {
    throw new Error(
      `ID token audience mismatch: expected ${options.clientId}, got ${JSON.stringify(claims.aud)}`
    );
  }
  const nowSec = Math.floor(now() / 1e3);
  if (typeof claims.exp === "number" && claims.exp + tolerance < nowSec) {
    throw new Error("ID token is expired");
  }
  if (typeof claims.iat === "number" && claims.iat - tolerance > nowSec) {
    throw new Error("ID token issued in the future");
  }
  if (options.nonce !== void 0 && claims.nonce !== options.nonce) {
    throw new Error("ID token nonce mismatch");
  }
  return claims;
}

// src/browser.ts
import { spawn } from "node:child_process";
var SystemBrowserLauncher = class {
  async open(url) {
    const command = process.platform === "darwin" ? { file: "open", args: [url] } : process.platform === "win32" ? { file: "cmd", args: ["/c", "start", "", url] } : { file: "xdg-open", args: [url] };
    await new Promise((resolve, reject) => {
      const child = spawn(command.file, command.args, { stdio: "ignore", detached: true });
      child.once("error", reject);
      child.once("spawn", () => {
        child.unref();
        resolve();
      });
    });
  }
};

// src/authorization.ts
import { join } from "node:path";
async function authorize(request, deps) {
  const config = resolveConfig(deps.config);
  const browser = deps.browser ?? new SystemBrowserLauncher();
  const hostId = await loadOrCreateHostId(join(config.storeDir, "host_id"));
  let idTokenHint = request.idTokenHint;
  if (request.existingClientId && !idTokenHint) {
    const existing = await deps.store.get(request.existingClientId);
    idTokenHint = existing?.idToken;
  }
  const { verifier, challenge } = createPkce();
  const state = randomToken(16);
  const nonce = randomToken(16);
  const callback = await startCallbackServer({
    host: config.callbackHost,
    port: config.callbackPort,
    signal: deps.signal
  });
  try {
    const authorizeUrl = buildAuthorizeUrl({
      clientId: request.existingClientId,
      extAgentHostId: hostId,
      agentNameHint: request.existingClientId ? void 0 : config.agentName,
      idTokenHint,
      loginHint: request.loginHint,
      redirectUri: callback.redirectUri,
      state,
      nonce,
      codeChallenge: challenge,
      resource: API_BASE_URL
    });
    const isRegistration = request.existingClientId === void 0;
    deps.onAuthorizeUrl?.(authorizeUrl, isRegistration);
    if (deps.openBrowser !== false) await browser.open(authorizeUrl);
    const result = await callback.waitForResult;
    if (result.state !== state) {
      throw new Error("authorization state mismatch; the callback was not bound to this attempt");
    }
    const issuedClientId = result.clientId ?? request.existingClientId;
    if (!issuedClientId) {
      throw new Error("registration incomplete: the callback carried no issued client_id");
    }
    if (result.clientId && request.existingClientId && result.clientId !== request.existingClientId) {
      throw new Error(
        "callback returned a different client_id than the pending registration; refusing to replace it"
      );
    }
    const tokens = await exchangeCode(
      {
        clientId: issuedClientId,
        code: result.code,
        codeVerifier: verifier,
        redirectUri: callback.redirectUri,
        // exact same URI as the authorize request
        resource: API_BASE_URL
      },
      { fetchImpl: deps.fetchImpl, timeoutMs: config.timeoutMs }
    );
    if (!tokens.id_token) throw new Error("token response carried no id_token");
    const claims = await verifyIdToken(tokens.id_token, {
      clientId: issuedClientId,
      nonce,
      fetchImpl: deps.fetchImpl
    });
    const credential = toCredential({
      tokens,
      claims: { sub: claims.sub, email: claims.email ?? null, iss: claims.iss },
      clientId: issuedClientId,
      hostId
    });
    await deps.store.save(credential);
    return {
      status: "authorized",
      credential,
      planUsageEnabled: credential.scopes.includes(PLAN_USAGE_SCOPE)
    };
  } finally {
    callback.close();
  }
}
function toCredential(input) {
  const { tokens } = input;
  const scopes = (tokens.scope ?? "").split(" ").filter(Boolean);
  const earliest = tokens.earliest_refresh_at ? Date.parse(tokens.earliest_refresh_at) : null;
  return {
    email: input.claims.email,
    issuer: input.claims.iss,
    subject: input.claims.sub,
    clientId: input.clientId,
    extAgentHostId: input.hostId,
    idToken: tokens.id_token ?? "",
    accessToken: tokens.access_token,
    refreshToken: tokens.refresh_token ?? "",
    tokenType: tokens.token_type,
    scopes,
    expiresAt: Date.now() + (tokens.expires_in ?? 0) * 1e3,
    earliestRefreshAt: Number.isFinite(earliest) ? earliest : null,
    savedAt: (/* @__PURE__ */ new Date()).toISOString()
  };
}
var refreshChains = /* @__PURE__ */ new Map();
async function ensureFreshCredential(clientId, deps) {
  const credential = await deps.store.get(clientId);
  if (!credential) throw new Error(`no stored credential for client ${clientId}`);
  const config = resolveConfig(deps.config);
  const refreshAt = credential.earliestRefreshAt ?? credential.expiresAt - config.refreshLeadMs;
  if (Date.now() < refreshAt) return credential;
  const previous = refreshChains.get(clientId) ?? Promise.resolve(credential);
  const next = previous.catch(() => credential).then(() => performRefresh(credential, deps));
  refreshChains.set(
    clientId,
    next.finally(() => {
      if (refreshChains.get(clientId) === next) refreshChains.delete(clientId);
    })
  );
  return next;
}
async function performRefresh(current, deps) {
  const latest = await deps.store.get(current.clientId) ?? current;
  const config = resolveConfig(deps.config);
  try {
    const tokens = await refreshToken(
      {
        clientId: latest.clientId,
        refreshToken: latest.refreshToken,
        resource: API_BASE_URL
      },
      { fetchImpl: deps.fetchImpl, timeoutMs: config.timeoutMs }
    );
    let claims = {
      sub: latest.subject,
      email: latest.email,
      iss: latest.issuer
    };
    if (tokens.id_token) {
      const verified = await verifyIdToken(tokens.id_token, {
        clientId: latest.clientId,
        fetchImpl: deps.fetchImpl
      });
      if (verified.sub !== latest.subject) {
        throw new Error("refreshed ID token belongs to a different account; refusing to replace");
      }
      claims = { sub: verified.sub, email: verified.email ?? latest.email, iss: verified.iss };
    }
    const updated = toCredential({
      tokens: { ...tokens, refresh_token: tokens.refresh_token ?? latest.refreshToken },
      claims,
      clientId: latest.clientId,
      hostId: latest.extAgentHostId
    });
    await deps.store.save(updated);
    return updated;
  } catch (error) {
    if (isUnusableRefreshError(error)) {
      throw new UnusableCredentialError(latest.clientId, error);
    }
    throw error;
  }
}
var UnusableCredentialError = class extends Error {
  clientId;
  cause;
  constructor(clientId, cause) {
    super(`credential for ${clientId} is no longer refreshable; sign in again`);
    this.name = "UnusableCredentialError";
    this.clientId = clientId;
    this.cause = cause;
  }
};
function planUsageEnabled(credential) {
  return credential.scopes.includes(PLAN_USAGE_SCOPE);
}

// src/store.ts
import { createHash as createHash3 } from "node:crypto";
import { mkdir as mkdir2, readFile as readFile2, writeFile as writeFile2, rename, readdir, unlink, chmod } from "node:fs/promises";
import { join as join2 } from "node:path";
function fileKey(clientId) {
  return createHash3("sha256").update(clientId).digest("hex").slice(0, 32);
}
var FileCredentialStore = class {
  #dir;
  constructor(dir) {
    this.#dir = join2(dir, "credentials");
  }
  #path(clientId) {
    return join2(this.#dir, `${fileKey(clientId)}.json`);
  }
  async get(clientId) {
    try {
      const raw = await readFile2(this.#path(clientId), "utf8");
      const parsed = JSON.parse(raw);
      if (parsed.clientId !== clientId) return null;
      return parsed;
    } catch (error) {
      if (error.code === "ENOENT") return null;
      throw error;
    }
  }
  async list() {
    let names;
    try {
      names = await readdir(this.#dir);
    } catch (error) {
      if (error.code === "ENOENT") return [];
      throw error;
    }
    const out = [];
    for (const name2 of names) {
      if (!name2.endsWith(".json")) continue;
      try {
        out.push(JSON.parse(await readFile2(join2(this.#dir, name2), "utf8")));
      } catch {
      }
    }
    return out.sort((a, b) => a.savedAt.localeCompare(b.savedAt));
  }
  async save(credential) {
    await mkdir2(this.#dir, { recursive: true, mode: 448 });
    const target = this.#path(credential.clientId);
    const tmp = `${target}.${process.pid}.tmp`;
    await writeFile2(tmp, JSON.stringify(credential, null, 2), { mode: 384 });
    await chmod(tmp, 384);
    await rename(tmp, target);
  }
  async remove(clientId) {
    try {
      await unlink(this.#path(clientId));
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
};

// src/main.ts
var name = "llm-siwc";
var inject = ["llm", "authorization"];
var Config = z.object({
  /** Provider route name requests select with `GenerateOptions.provider`. */
  provider: z.string().default("chatgpt"),
  /** Model ids to advertise in pickers. */
  models: z.array(z.string()).default([
    "gpt-6.1-sol",
    "gpt-6-astra",
    "gpt-6-sol",
    "gpt-6-luna",
    "gpt-5.6-sol",
    "gpt-5.6-terra",
    "gpt-5.6-luna"
  ]),
  /** Where SIWC credentials and the host id are persisted. */
  storeDir: z.string().default(""),
  /** Loopback callback host; keep 127.0.0.1 (docs forbid `localhost`). */
  callbackHost: z.string().default("127.0.0.1"),
  /** Preferred loopback callback port. */
  callbackPort: z.natural().default(1455),
  /** Refresh this many ms before access-token expiry. */
  refreshLeadMs: z.natural().default(3e5),
  /** Internal name for the flow registry. */
  flowId: z.string().default("chatgpt"),
  /** Label shown to the user on the sign-in surface. */
  flowLabel: z.string().default("ChatGPT")
});
function defaultStoreDir2() {
  const home = process.env.DSH_HOME ?? `${process.env.HOME ?? ""}/.dsh`;
  return join3(home, "siwc");
}
var HarnessSiwcAdapter = class extends LlmAdapter {
  #core;
  constructor(core) {
    super();
    this.#core = core;
  }
  providerInfo(provider) {
    return this.#core.providerInfo(provider);
  }
  listModels(provider) {
    return Promise.resolve(this.#core.listModels(provider));
  }
  resolveModel(provider, model) {
    return this.#core.resolveModel(provider, model);
  }
  stream(options) {
    return this.#core.stream(options);
  }
};
function apply(ctx, config) {
  console.log(`llm-siwc: apply() entered (provider=${config.provider}, models=${config.models.length})`);
  const settings = resolveConfig({
    storeDir: config.storeDir === "" ? defaultStoreDir2() : config.storeDir,
    callbackHost: config.callbackHost,
    callbackPort: config.callbackPort,
    refreshLeadMs: config.refreshLeadMs
  });
  const store = new FileCredentialStore(settings.storeDir);
  const hostIdPath = join3(settings.storeDir, "host_id");
  void loadOrCreateHostId(hostIdPath).catch((error) => {
    console.error("llm-siwc: could not persist the host id:", error);
  });
  const core = new SiwcResponsesAdapter({
    providers: [config.provider],
    models: config.models,
    resolveAccessToken: async (provider) => {
      const credential = await pickCredential(store, provider);
      if (!credential) {
        throw new Error(
          `no ChatGPT credential for provider "${provider}"; sign in with ChatGPT first`
        );
      }
      if (!planUsageEnabled(credential)) {
        throw new Error(
          "the stored ChatGPT credential lacks the chatgpt.tokens.use.direct scope; reauthorize with the full scope set"
        );
      }
      const fresh = await ensureFreshCredential(credential.clientId, { store, config: settings });
      return fresh.accessToken;
    }
  });
  try {
    ctx.llm.registerAdapter([config.provider], new HarnessSiwcAdapter(core));
    console.log(`llm-siwc: LLM route "${config.provider}" registered`);
  } catch (error) {
    console.error(`llm-siwc: could not register the "${config.provider}" route:`, error);
    return;
  }
  const authorization = ctx.authorization;
  if (authorization === void 0) {
    console.warn(
      "llm-siwc: no authorization service mounted; inference works, but ChatGPT sign-in is unavailable"
    );
    return;
  }
  try {
    authorization.registerFlow({
      key: credentialKey("llm-siwc", config.flowId),
      label: config.flowLabel,
      methods: [{ id: "oauth", label: "Continue with ChatGPT" }],
      async run(session) {
        const result = await authorize(
          {},
          {
            store,
            config: settings,
            signal: session.signal,
            browser: {
              open: async (url) => {
                session.notify({ message: "Continue signing in to ChatGPT in your browser.", url });
              }
            }
          }
        );
        if (result.credential.email) {
          session.notify({ message: `Signed in as ${result.credential.email}.` });
        }
        if (!planUsageEnabled(result.credential)) {
          session.notify({
            message: "Sign-in succeeded, but ChatGPT plan usage was not granted. Reauthorize with the full scope set or configure another billing path."
          });
        }
        await commitRecord(session, result.credential, config);
      }
    });
    console.log("llm-siwc: apply() completed \u2014 sign-in flow registered");
  } catch (error) {
    console.error("llm-siwc: could not register the sign-in flow:", error);
  }
}
async function pickCredential(store, provider) {
  const all = await store.list();
  if (all.length === 0) return null;
  const usable = all.filter((credential) => credential.clientId.startsWith("oaiapp_"));
  return usable.at(-1) ?? null;
}
async function commitRecord(session, credential, config) {
  const record = {
    type: "oauth",
    access: credential.accessToken,
    refresh: credential.refreshToken,
    expires: credential.expiresAt,
    accountId: credential.subject
  };
  try {
    await session.commit(record);
  } catch (error) {
    throw new Error(
      `ChatGPT sign-in succeeded and the credential was stored, but recording it with the credential service failed: ${String(error)}`
    );
  }
}
export {
  Config,
  FileCredentialStore,
  SiwcResponsesAdapter,
  apply,
  authorize,
  ensureFreshCredential,
  inject,
  name
};
