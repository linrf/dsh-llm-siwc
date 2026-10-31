# DSH 真实工作流验收与首轮提前结束修复（2026-11-01）

范围：DSH desktop `0.2.1-rc.5`，本地 link 的 `dsh-llm-siwc`，ChatGPT (VM gateway) / GPT-6.1-Sol / Low，VM gateway 2.3.0。仅在公开代码仓库工作区新建测试会话，不读取账号文件、不操作其他会话。测试前未把简单回复或仅 HTTP200 当作验收通过。

## 首轮问题及根因

旧测试 session `session-8cb7ab51-4a24-43bc-ae33-27cf9f03ccce` 首轮只显示“会读取源码”的计划即结束。第一次仅看最终 `message.content` 与 `finish stop` 时，曾把它误判成模型没有调用工具。重新核对 canonical stream 后，该判断已纠正。

首轮实际事件顺序：

```text
block-start text index0
block-end   text index0
block-start tool-call index0
工具参数 chunks，name=run_code
block-end   tool-call index0
finish stop
```

最终 materialized message 只有 text，没有 tool-call，也没有对应工具派发。原始模型工具调用存在，丢失发生在插件输出到宿主组装的路径。

- 插件旧实现对文本直接使用上游 `output_index`，对工具却使用从0开始的另一套 `nextIndex()`；两种编号混用，文本与工具都得到0。
- DSH `BlockAssembler` 对同一index只接受第一个 `block-end`；已关闭的块忽略后续delta、start和end。这是宿主明确的组装规则。
- agent-loop 依据最终message中是否存在tool-call决定继续；工具块丢失后就结束本轮。不是根据“工具错误”或“断网”结束。
- 另外，旧插件以 `call_id` 建工具映射，却用上游 `item_id` 查参数增量；当两者不同时会丢失增量。最终done携带完整参数的场景可能掩盖它。

安全证据：[首轮修复前元数据](evidence/gateway-2.3.0-dsh-first-turn-before.json)。只保留事件类型、index、工具名和配对布尔值，没有原始正文、工具参数、账号或令牌。

宿主参考代码：`deepseek-harness/packages/llm/llm/src/assembler.ts`（first-close-wins）和 `packages/core/agent-loop/src/agent.ts`（从最终content判断工具调用）。首轮实际日志提供了运行时证据，不只依赖源码推测。

## 修复和复验

`src/adapter.ts` 统一为所有文本、思考、工具块分配本地index，并分别按上游output/content/summary part定位同一块。工具支持原生item.id寻址参数增量，call_id用于工具结果配对；已关闭块忽略迟到事件。finish、session headers、usage与取消语义保持不变。

`test/plugin.test.ts` 新增3个回归，原错误delta fixture改用item_id而非call_id；44/44通过，`npm run build`成功，并更新已提交分发的 `lib/main.js`。真实宿主的BlockAssembler缩减事件回放也显示：旧编号只保留text，修复后保留text+tool-call。

DSH空闲后退出并重开，加载本地link的新lib。修复后新session `session-22ba9e94-c1cf-4056-86f4-cfb3bf6c29d7` 使用同一工作区、相同模型/effort及完全相同的首条用户prompt（SHA256为 `8fcb57778a7e4e742b74158e229a4e88f2365c3d32a4f432e8bf99c751b464dd`）。系统上下文包含时间等动态内容，不能称完全相同请求。

新会话首step的text index0、tool index1，最终message同时保留两块，run_code实际派发且结果成功，不需额外“继续”消息。后续完整多轮结果在执行后补充；旧会话首轮失败保留，不归档为无条件通过。


第三轮在真实长SSE已显示文字后，主Agent通过DSH停止按钮主动取消。canonical记录 `turn/end.reason.kind=aborted`、`reason.kind=user`；VM对应请求 `26552d5d` 记录 `client-disconnected`（约18.80s），取消后inflight、队列、upload/validator/FD、spool和active streams均为0。此轮是预期取消，不作为模型/网络失败，也不记为正常完成。见 [取消证据](evidence/gateway-2.3.0-dsh-cancel-vm.json)。


## 多轮桌面与VM结果

- 修复后同一会话完成4轮/12次模型step：第1、2、4轮正常完成，第3轮主动取消；不是把取消也记为正常完成。
- 第1轮实际分段读取网关、normalizer、client、adapter、convert与测试，回答调用链及边界，无需补发“继续”。
- 第2轮在DSH工具内实际运行 `npm test`（44/0/0，exit0）与指定网关定向测试（7/0/0，exit0）。数字来自实际tool/job结果，未匹配用例不冒充执行通过。
- 第3轮真实SSE已显示文字后点击停止，VM取消与资源归零见上。
- 第4轮恢复后继续执行只读工具，正确回忆44/0/0、7/0/0及exit0，说明索引修复、401重放、SSE不能盲重放及两类ID区别，没有重跑测试。
- UI显示累计约715K tokens、缓存命中89%、上下文16%；这是本会话投影结果，不是长期缓存保证。轨迹页能显示首轮文本后的实际工具调用与结果，随后已返回最终对话供查看。
- VM时间窗口内共17个Responses请求（包含DSH标题/辅助请求，不与主会话12steps等同）：全部上游200，16条正常完整流，1条主动取消，无responses-failed。最终inflight、所有队列、upload/normalizer/upload FD/spool/SSE均为0，服务active/running，NRestarts=0，OOM=0。
- 本次实际正文最大319,524 bytes（约312KiB），属于真实多轮工具工作流。**没有把它称为20–30MiB真实模型上下文验收**；大体积支持仍以此前20/30/50MiB合成传输与受限内存验证为证。没有触发真实账号401/429；这些恢复路径由本地loopback覆盖。

证据：`docs/evidence/gateway-2.3.0-dsh-session.json`、`gateway-2.3.0-dsh-vm.json`、`gateway-2.3.0-dsh-plugin-tests.txt`，均不保存原始会话正文、工具参数、账号或令牌。

本地profile link核对：`~/.dsh/profiles/desktop/node_modules/dsh-llm-siwc`解析到本仓库，构建产物一致；`lib/main.js` SHA256为 `4e189f00a4c518c513ce69fc10c8a33b8da540c08352af9de3bc2a3dbd647ec2`。此次只重启DSH加载插件；VM源码、Codex应用、客户端provider认证和本机网络配置未改。

最终canonical逐step核对：8个根工具调用与8个结果、37个PTC子调用（read31/bash3/grep2/job_output1），工具错误0。12steps均无stream→message→dispatch→result的工具ID缺失或block index复用；轮次、step、pending calls均闭合。第2/4轮缓存命中分别约96.62%/97.98%，属于本次观测。
