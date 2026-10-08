// src/bootstrap.ts
import { registerHooks, createRequire } from "node:module";
import { existsSync } from "node:fs";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
var SCOPE = "@deepseek-ai/";
var PEER_PACKAGES = /* @__PURE__ */ new Set([
  "@deepseek-ai/cordis",
  "@deepseek-ai/dsh-llm",
  "@deepseek-ai/dsh-credentials",
  "@deepseek-ai/dsh-authorization",
  "@deepseek-ai/schemastery"
]);
function packageNameOf(specifier) {
  const parts = specifier.split("/");
  return specifier.startsWith("@") ? parts.slice(0, 2).join("/") : parts[0] ?? specifier;
}
var hostRoot = null;
function candidateRoots() {
  const roots = [];
  const fromEnv = process.env.DSH_HOST_ROOT;
  if (fromEnv !== void 0 && fromEnv !== "") roots.push(fromEnv);
  const executable = process.execPath;
  roots.push(resolve(dirname(executable), "..", "Resources", "app.asar", "dsh"));
  roots.push(resolve(dirname(executable), "..", "Resources", "app", "dsh"));
  roots.push(resolve(dirname(executable), "resources", "app.asar", "dsh"));
  roots.push(resolve(dirname(executable), "resources", "app", "dsh"));
  roots.push(resolve(dirname(executable), "..", "..", "app.asar", "dsh"));
  return roots;
}
async function probe(root) {
  const anchor = join(root, "package.json");
  try {
    const entry = createRequire(anchor).resolve(`${SCOPE}dsh-llm`);
    const url = isAbsolute(entry) ? pathToFileURL(entry).href : pathToFileURL(join(root, entry)).href;
    await import(url);
    return true;
  } catch (error) {
    const code = error.code;
    if (code === "ERR_MODULE_NOT_FOUND" || code === "ENOENT" || code === "ENOTDIR") return false;
    return true;
  }
}
async function discoverHostRoot() {
  const candidates = candidateRoots();
  for (const candidate of candidates) {
    if (await probe(candidate)) return candidate;
  }
  for (const candidate of candidates) {
    if (existsSync(join(candidate, "node_modules", SCOPE, "dsh-llm"))) return candidate;
  }
  return null;
}
var installed = false;
async function initPeerResolution() {
  if (installed) return;
  installed = true;
  hostRoot = await discoverHostRoot();
  if (hostRoot === null) {
    process.emitWarning(
      "dsh-llm-siwc: could not locate the DSH installation; set DSH_HOST_ROOT to its dsh package directory"
    );
  }
  registerHooks({
    resolve(specifier, context, nextResolve) {
      if (!specifier.startsWith(SCOPE)) return nextResolve(specifier, context);
      if (!PEER_PACKAGES.has(packageNameOf(specifier))) return nextResolve(specifier, context);
      const root = hostRoot;
      if (root === null) return nextResolve(specifier, context);
      try {
        const resolved = createRequire(join(root, "package.json")).resolve(specifier);
        const url = isAbsolute(resolved) ? pathToFileURL(resolved).href : pathToFileURL(join(root, resolved)).href;
        return { url, shortCircuit: true };
      } catch {
        return nextResolve(specifier, context);
      }
    }
  });
}

// src/index.ts
await initPeerResolution();
var main = await import("./main.js");
var name = main.name;
var inject = main.inject;
var apply = main.apply;
var Config = main.Config;
export {
  Config,
  apply,
  inject,
  name
};
