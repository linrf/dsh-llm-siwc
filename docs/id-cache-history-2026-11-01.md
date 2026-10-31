# ID 与 Prompt 缓存历史核对（2026-11-01）

核对范围：当前仓库 `5f321c2`、提交 `e0ea989` 的实际 diff、2026-10-08 原始 Codex 记录及当时保存的测试结果。本文不包含令牌、请求正文或账号标识。

## 结论

2026-10-08 因低缓存命中率增加的是**稳定的 HTTP `session-id` 请求头**。用户随后补充记忆为“preview-session-id，传入会报错”；对照 10/31 原始 DSH 记录，很可能指 **JSON `previous_response_id`**。后者是 HTTP 会话续接参数，与缓存亲和性请求头不同。缓存修复当时没有删除请求中的 `item.id`，也没有新增 `previous_response_id`。本次流式上传改造必须保留缓存请求头与工具关联字段的原有语义。

当前网关 2.2.0 只删除 `UNSUPPORTED` 中列出的顶层参数，并强制 `store:false`、`stream:true`；没有通用的 `id` 删除逻辑。DSH 的请求转换也没有遍历删除 `id`。它构造新的 input 项，工具调用原始 block 的 `id` 被用作 `function_call.call_id`，工具结果使用相同 `call_id` 配对。

## 原始记录与反事实对照

原始 Codex thread：`01a11bf3-3690-7611-a1fc-fe534ec9cedc`。记录位于本机 `~/.codex/sessions/2026/10/08/rollout-2026-10-08T22-38-06-01a11bf3-3690-7611-a1fc-fe534ec9cedc.jsonl.zst`。以下时间均为 Asia/Taipei（UTC+08:00）。

- **2026-10-08 23:23**：33 次请求的调查结论指出，缺少稳定 `session-id` 是可复现的缓存损失因素。对照使用同一账号、同一 HTTP/SSE 端点，保持 `store:false`；原插件仅在实验组注入请求头，各对应轮次的正文 SHA256 相同。
- 三轮会话结果：Luna 不带请求头为 `0 → 0 → 0` cached tokens，固定请求头为 `0 → 6912 → 6912`；Sol 不带请求头为 `0 → 0 → 0`，固定请求头为 `0 → 7168 → 0`。固定请求头后仍有零命中，不能据此保证每轮命中或解释所有历史损失。
- **2026-10-08 23:28**：插件已增加 `sessionId` 透传，40 项测试及构建通过；直接调用修改后 adapter 的短文本验收中，首次冷请求之后，Luna 后两轮约 94%，Sol 约 97%。
- **2026-10-08 23:35**：重启后的真实 DSH 桌面会话读回记录显示，Sol 第 2、3 轮分别为 **98.27% / 97.97%**；切换 Luna 后第 1 轮冷请求为 0，第 2 轮为 **96.98%**。这是一个小规模、纯文本验收；没有桌面 HTTP 抓包，也不代表长期命中率。

当时的本机证据（此次重新读取核对，未复制原始会话）：

- `/Users/bytedance/Documents/Codex/2026-10-08/du/outputs/cache_followup_findings.json:115`：相同正文的 adapter 对照，`matched_body_sha256_each_turn: true`。
- `/Users/bytedance/Documents/Codex/2026-10-08/du/outputs/cache_patched_adapter_probes.json`：修改后的 adapter 请求头、顶层 usage、正文 hash 与结果。
- `/Users/bytedance/Documents/Codex/2026-10-08/du/outputs/cache_desktop_acceptance.json:24`：真实桌面会话第 2 轮起的 token 统计与验收限制。

## 代码核对

提交 `e0ea989b5c2e4ed48f748088150c4d7c4a39f6c6`（2026-10-08 23:37 +08:00）标题为 `Fix prompt cache affinity with stable session headers`。实际 diff 只增加 `ResponsesRequest.sessionId`、adapter 参数透传和 HTTP 请求头，以及相应测试、文档和构建产物；没有 body ID 删除改动。

| 字段 | 用途与本次处理要求 | 当前实现位置（改造前行号） |
|---|---|---|
| HTTP `session-id` | 同一会话保持稳定，用于缓存路由亲和性；不同于每次请求的追踪 ID。必须保留，包括 401 重试。 | `src/adapter.ts:295`、`src/client.ts:123`；gateway `FORWARD_HEADERS` 和 `openUpstreamStream()` |
| JSON `previous_response_id` | 引用服务端保存的上一响应；本插件从首个提交起就不使用，代码注释原因是 SIWC HTTP 路由拒绝，改用完整历史重放。并非 10/8 缓存修复增加的字段。 | `src/adapter.ts:12`（`git blame` 指向初始提交 `1e4c90a`） |
| input item 的 `id` | 响应项标识；不等同于会话请求头，也不等同于工具 `call_id`。当前 gateway 未通用删除。 | `gateway/siwc-gateway-v2.mjs:88` 附近仅删除顶层 `UNSUPPORTED` |
| 工具 `call_id` | 将 function call 和 function call output 配对，必须保留。 | `src/convert.ts:103`、`:118` |
| JSON `prompt_cache_key` | 另一类缓存相关参数；不是当次缺失的 HTTP 请求头。本次未证明可以用它替代稳定 `session-id`。 | 10/8 调查记录与 `cache_followup_findings.json` |

仓库说明 `README.zh.md:105` 已说明稳定 `session-id`、冷请求和服务端波动的边界。网关的请求头白名单包含 `session-id`，因此 DSH → VM → 上游会延续该值。

## “传入会报错”的原始语境与证据边界

还核对了当前 thread `01a19267-3ca6-7421-a1d2-33137581361f` 在 2026-11-01 01:25 的上一轮最终解释。该解释提到删除不受支持的字段，但没有指明删除某一种 `id`。

进一步读取目标 DSH 的**完整压缩原始日志**，发现了用户所指的近似原话。文件为 `/Users/bytedance/.dsh/sessions/--Users-bytedance-Documents-deepseek-harness-default-workspace--/session-362d9293-59ed-4062-b13c-57c8ec2d6fd3/session.v4.jsonl.zstd`，以下行号指解压后的 JSONL：

- **2026-10-31 18:27:11 +08:00，line 556 / seq 554**：assistant 讨论 Codex 可能发送 `previous_response_id`，认为该路由 HTTP 下不支持，提出在完整历史已经存在于 `input` 时移除该字段。这段是设计分析，不是上游错误响应。
- **2026-10-31 18:28:41 +08:00，line 614 / seq 612**：assistant 最终解释明确写了“`previous_response_id` 不可用”，并说网关需做好字段剥离，否则 Codex 带这个字段会报错。这里没有说 `session-id` 缓存请求头会报错。
- 初次仅检查 `session_projcache` 投影缓存会遗漏这部分历史。本文改以完整原始日志为依据。

更早的 DSH `session-983ab5fe-eb24-46bc-be84-e956527e410f` 在 **2026-10-08 17:11:15 +08:00，解压 line 770 / seq 768** 已获取官方 preview limitations，其要求是：

> Omit `previous_response_id` over HTTP and send the required history in `input`.

2026-11-01 重新打开的 [SIWC 官方 preview limitations](https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations) 仍有此要求。该页另外说明，WebSocket 续接只能引用同一认证连接上的响应，并不提供持久会话存储。因此，这不是所有 OpenAI Responses 路由一概不支持，而是当前 SIWC HTTP 路由的兼容约束。

在上述两份 DSH 原始日志及 10/8 Codex 排查记录中，**未找到一次实际发送 `previous_response_id` 后返回的 HTTP 400 原文、错误 code 或 request ID**。可以确认官方要求省略，也可以确认当时向用户作过“会报错”的解释；不能把该解释升级为已复现的具体 400 错误。搜索范围内也未找到正式参数 `preview-session-id`。

实现约束据此明确为：保留稳定 HTTP `session-id` 与工具 `call_id`；对 SIWC HTTP 的 `previous_response_id` 单独按兼容策略处理。**仅删除 `previous_response_id` 并不能恢复缺失历史**，若客户端只发送增量 input，网关不能假装拥有完整会话；不能笼统写成“删除 ID”。

## 网关 2.3 的处理决定

2.3 对请求体顶层 `previous_response_id` 明确返回本地 HTTP 400，并提示调用方省略该字段、在 `input` 中发送完整的所需历史。网关不静默删除它，因为网关无法判断并补齐增量客户端省略的历史。DSH 当前自行构造的请求不包含该字段，因此不受这条兼容校验影响。

HTTP `session-id`、`conversation_id` 继续透传；input item 的 `id` 和工具 `call_id` 保留。401 刷新后的重放也使用同一组会话请求头，并纳入回归测试。

这是本次 2.3 新增的本地明确校验及测试要求；上文关于 2.2 的代码范围和历史证据保持不变，不能据此追认以前已经实测过上游 400。2.3 的具体测试与部署结果以本次改造验收记录为准。
