[English](README.md) | 中文

# dsh-llm-siwc

一个 DeepSeek Harness **插件**，通过 OpenAI 官方的
[Sign in with ChatGPT](https://developers.openai.com/siwc)（SIWC）流程接入
ChatGPT 套餐推理。它是由运行中的应用从 profile 加载的独立 bundle，**无需重新编译
DSH**。

已针对线上服务验证，日期 2026-10-08。

## 环境要求

- DSH **0.2.0-rc.2** —— 插件声明 `>=0.2.0-rc.2 <0.3.0` 的 peer 范围，DSH 会拒绝
  加载 peer 范围与运行时不符的插件
- 一个具备 plan usage 资格的 ChatGPT 套餐
- Node 22.19+ 仅用于从源码构建；安装发布版不需要构建

## 安装

**安装这个包本身就是全部安装步骤**：`lib/` 已提交到仓库，包内也声明了 bundle
patch，因此本机不会执行任何编译。

### 桌面应用 —— 常规方式

使用应用自带的 **插件** 页面。这是 desktop profile 唯一受支持的路径，不需要命令
行，也不需要手工编辑配置：

1. 在侧边栏打开 **插件**。
2. 在安装输入框粘贴本仓库地址：
   `https://github.com/linrf/dsh-llm-siwc`
3. 安装，若新增的 bundle 未开启则将其开启。

该页面会读取 spec、调用 profile 的包管理器、展示其输出，并激活新增内容。卸载时会
要求确认。

之后**重启应用**以加载新 bundle。

### 命令行 —— 其他任意 profile

```bash
dsh plugin --profile <name> add https://github.com/linrf/dsh-llm-siwc
```

已做端到端验证：pnpm 解析出 `github:linrf/dsh-llm-siwc`，bundle 写入 profile，
`node_modules/dsh-llm-siwc/lib` 完整到位，且 `dsh --profile <name> --dump-config`
能列出 `llm-siwc` 行。本地检出同样适用，用路径或 `link:` spec 即可：

```bash
dsh plugin --profile <name> add /absolute/path/to/dsh-llm-siwc
```

`scripts/install.sh` 是 desktop profile 的**兜底方案**：它直接编辑 profile 的
`package.json` 并 link 该包。请优先使用 **插件** 页面，该脚本只在页面不可用时才需要。

## 使用方法

### 登录

**在 GUI 中** —— 在输入框输入 `/chatgpt login`：

```
/chatgpt           显示当前登录账号（等同于 /chatgpt status）
/chatgpt status    账号、client id、plan usage 授权范围、access token 过期时间
/chatgpt login     打开浏览器并执行官方 SIWC 授权流程
/chatgpt logout    撤销会话并清除本地凭据
```

浏览器往返在后台执行，因此 `/chatgpt login` 会立即返回；授权完成后用
`/chatgpt status` 确认。

**在命令行中** —— 不需要会话：

```bash
node scripts/login.mjs            # 登录
node scripts/login.mjs --status   # 查看已存账号
node scripts/login.mjs --logout   # 撤销并清除
```

### 选择模型

在输入框的模型控件（`/model`）中选择一个 **ChatGPT** 模型。该列表读取自路由自身的
目录接口 `GET /v1/models`，因此反映账号**实际可用**的模型 —— 撰写时为 10 个，包括
`gpt-reserve`、`gpt-5.5` 与 `codex-auto-review`。

目录同时提供每个模型的真实显示名、描述、上下文窗口、输入模态，以及**它自己支持的
推理强度**。这些强度因模型而异（`gpt-5.5` 最高到 `xhigh`，`gpt-6.1-sol` 可达
`ultra`），这正是插件去读取它们、而非内置一份共用列表的原因。所选强度会作为
`reasoning.effort` 发送到请求。

该读取缓存五分钟，并合并并发请求。若读取失败（离线、凭据过期、预览服务中断），
选择器会退回一份**刻意保守**的内置列表而不是变为空白，下一次读取成功后即替换。

设置 → 模型 中也会出现 **ChatGPT** 行。其字段仅作展示（显示名、base URL 覆盖），
不配置也不影响路由工作。

### 关于该命令的说明

- 命令结果**按设计渲染在模型历史之外** —— 它不是聊天消息，模型看不到它，也不消耗
  token。
- 命令绑定到接收它的 agent，因此**请在会话中运行**。在没有会话的新窗口里，请改用
  `scripts/login.mjs`。
- 该命令刻意不声明 `input` 描述符：带 `input` 的 host 描述符会被解析为
  `leadingInput`（输入框会等待你继续输入），使单独的 `/chatgpt` 看起来毫无反应。

## 注册了什么

| 注册项 | 作用 |
|---|---|
| 授权流程 `llm-siwc/chatgpt` | 任意界面都可发起 "Continue with ChatGPT" 登录 |
| LLM 适配器路由 `chatgpt` | 通过 `https://api.openai.com/v1/responses` 推理 |
| `/chatgpt` 命令 | GUI 内登录、账号状态查询、登出 |
| Provider 目录项 | 设置 → 模型 中显示 **ChatGPT** 行 |

插件自带凭据存储，因为一份 SIWC 注册携带了通用凭据记录未建模的字段：
`client_id`、`ext_agent_host_id`、`id_token` 以及授予的 scope。

### 该路由支持的能力

| 能力 | 行为 |
|---|---|
| 流式输出 | SSE 增量映射为 harness 的 `StreamChunk` |
| 工具调用 | `function_call` / `function_call_output` 完整往返，支持单轮多次 |
| 工作流（PTC） | 模型以工具参数形式生成工作流 JavaScript；`agent()` / `parallel()` 已验证 |
| 图片 | 每次请求从附件服务读取，以 base64 data URL 发送 |
| 模型目录 | 实时读取 `GET /v1/models`；含每个模型的名称、上下文、模态、推理强度 |
| 推理强度 | 使用每个模型自身支持的档位，作为 `reasoning.effort` 发送 |
| 用量统计 | 计数映射为 harness 的 camelCase 结构，会话可正常投影 |
| 重试策略 | 只重试瞬时故障 —— **绝不**重试额度超限的 429 |

## 限制

### 预览接口限制

Responses 路由会拒绝 `temperature`、`top_p`、`max_output_tokens`、`metadata`、
`prompt`、`truncation`、`user` 等字段 —— `stripUnsupportedFields()` 会将它们移除。
它同样拒绝显式的 `{"type":"message","role":"system"}` 条目（这类内容会被提升为
`instructions`），以及 HTTP 方式下的 `previous_response_id`（历史会被完整重放）。

**客户端 function 工具可正常工作。** 完整的 工具调用 → 工具结果 → 最终回答 往返
已线上验证；仅**托管**工具（Code Interpreter、文件检索、托管 MCP、`tool_search`）
不可用。

### 推理输出

除非显式请求摘要，路由不会发送任何推理文本，因此插件会请求
`reasoning.summary: auto` —— 可通过 `reasoningSummary` 配置，设为 `none` 即关闭。
有两个路由侧行为值得了解，均针对线上接口验证：

- **仅在请求不携带任何 tools 时**才会返回摘要。携带 tools 时推理增量根本不会到达，
  因此常规的带工具会话中思考流始终为空。插件仍会持续请求摘要，以便路由行为变化时
  自动生效。
- `effort: low` 即使不带 tools 也不产生摘要；`medium` 及以上才会。

### 重试策略

harness 默认会对 `RATE_LIMIT` 重试五次。但在本路由上，429 表示
`subscription_sharing_usage_limit_exceeded` —— 一个持续性的套餐限制，OpenAI 的文档
要求暂停该账号而非重复请求。因此本路由声明的策略只重试 `EMPTY_RESPONSE`、`SERVER`、
`TIMEOUT` 与 `TRANSPORT`。

## 设计说明

### peer 解析如何工作

插件的 peer（`@deepseek-ai/dsh-llm` 等）位于打包后的
`app.asar/dsh/node_modules` 内，从插件自身目录出发的常规 `node_modules` 解析无法
触达。`lib/bootstrap.js` 会发现正在运行的安装位置，并安装一个**同步**的 resolve
钩子，通过 `createRequire` 将 `@deepseek-ai/<name>` 映射过去，使入口文件完全按 node
的规则由 `exports`/`main` 决定。

有两条约束是关键性的，且很容易被改坏：

- resolve 钩子必须**同步** —— 异步钩子会让 node 拒绝该结果（`shortCircuit` 读作
  `undefined`）
- 钩子必须返回包的**入口文件**，而不是它的目录 —— ESM 不会像 CJS 那样把目录解析为
  manifest 入口

需要时可用 `DSH_HOST_ROOT` 覆盖发现逻辑。

### 协议要点（均已线上验证）

- `client_id=dynamic_agent_client` 是**注册入口**；回调返回签发的 `oaiapp_…`，令牌
  交换使用的正是它
- ID token 的 `aud` 是**数组**（`["oaiapp_…"]`）；access token 的 `aud` 是字符串
  `https://api.openai.com/v1`
- 回环回调必须是 `127.0.0.1`，绝不能用 `localhost`
- access token 有效期 **1 小时**，refresh token **30 天**；refresh token 会轮换，
  因此同一注册的刷新操作是串行的
- 推理必须使用 `api.openai.com/v1/responses`，并带 `store:false` 与 `stream:true`，
  **绝不能**使用 `chatgpt.com/backend-api`
- `ext_agent_host_id` 只生成一次并复用；重新生成后服务会把这台机器视为新主机

## 开发

```bash
pnpm install
pnpm build     # -> lib/bootstrap.js, lib/index.js, lib/main.js（已提交）
pnpm test      # 35 个测试
```

`scripts/build.mjs` 使用 esbuild 打包 `src/`。peer 导入（`@deepseek-ai/*`）保持
external，由 `lib/bootstrap.js` 在运行时解析。

测试覆盖消息转换（含 system 消息提升、工具调用/结果的配对）、SSE 重组、适配器流
映射、实时模型目录、用量转换、强制的 `store`/`stream` 标志、错误分类，以及一次贴近
真实形态的工具往返。

### 目录结构

```
src/
  bootstrap.ts      主机发现 + 同步 peer resolve 钩子
  index.ts          入口：初始化 bootstrap，再加载主体
  main.ts           插件主体：注册流程、适配器与 /chatgpt
  adapter.ts        Responses SSE -> harness StreamChunk
  client.ts         流式请求，执行预览接口约束
  catalog.ts        实时模型目录（名称、上下文、推理强度）
  credentials.ts    选取最新注册（无 peer 依赖，便于测试）
  convert.ts        harness 消息 -> Responses input 条目
  sse.ts            server-sent events 解析器
  errors.ts         错误矩阵 + 不支持字段的剔除
  authorization.ts  authorize / ensureFreshCredential / signOut
  oauth.ts          授权 URL、code 交换、刷新、撤销
  verify.ts         ID token 校验（JWKS、iss、aud、exp、nonce）
  callback.ts       回环监听器，含端口回退
  store.ts          凭据持久化（0600，原子写入）
  host-id.ts        ext_agent_host_id
  browser.ts        系统浏览器启动器
  config.ts         协议常量
test/                    35 个测试
scripts/build.mjs        esbuild 打包
scripts/install.sh       desktop profile 安装（兜底方案）
scripts/login.mjs        命令行登录 / 状态 / 登出
scripts/rollback.sh      应用出问题时移除插件
scripts/repair-usage.mjs 改写以 provider 结构写入的历史用量块
```

## 故障排查

若安装后会话无法创建，请执行回滚脚本，并查看 issue 列表：

```bash
bash scripts/rollback.sh    # 从 desktop profile 中移除插件
```

在 v0.1.0 之前的构建中创建的会话，其 `usage` 块可能是 provider 的蛇形结构，这会以
`uncachedInputTokens: NaN` 破坏会话投影。`scripts/repair-usage.mjs` 可就地改写
（默认仅试运行，每条日志备份为 `*.bak-usage`）：

```bash
node scripts/repair-usage.mjs           # 仅报告
node scripts/repair-usage.mjs --apply   # 备份并改写
```

## 免责声明

这是一个独立的、非官方的社区插件，与 OpenAI 或 DeepSeek 无附属、背书或支持关系。

它使用 OpenAI 官方文档记载的
[Sign in with ChatGPT](https://developers.openai.com/siwc) OAuth 流程，消耗已登录
用户**自己的** ChatGPT 套餐额度。除用户明确授权的注册信息外，它不存储任何凭据，也
从不会接触账号密码。

本路由上的 ChatGPT 套餐用量属于预览能力。资格、额度行为与可接受的请求形态随时可能
变化，且可能因账号而异。你有责任确认自己的账号符合资格，并遵守 OpenAI 的条款。
