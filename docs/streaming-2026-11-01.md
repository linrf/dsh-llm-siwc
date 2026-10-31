# SIWC gateway 2.3.0：边收边转与分阶段并发

日期：2026-11-01，Asia/Taipei。接续 [2.2.0 记录](changes-2026-11-01.md)。源码仍在本仓库 `gateway/`，不保存真实正文、bearer、OAuth 或 SSH key。

## 行为与代码

- `gateway/streaming-json.mjs`：新增增量 UTF-8/JSON 校验与顶层规范化。保留字段原始字节，不完整物化输入，不启动 JSON worker；保留消息 `id`、工具 `call_id`、数字和 escape 表达。
- `gateway/siwc-gateway-v2.mjs`：接收/解压/校验/写入私有文件，与上游 tail reader 重叠。reader 只读已写入部分；最终一个 `}` 必须等 JSON、压缩完整性、wire/decoded 限额全通过才开放。失败时上游可能已经收到部分数据，但不会提交成完整 JSON。
- producer 与每次 attempt 的 reader 分开。early 401 只取消首个 reader，继续接收并校验；完整后单次刷新并重放，重试使用实际 Content-Length。stable session headers 同样保留。其他错误或已输出 SSE 不自动重放。
- 分阶段：全生命周期响应槽 8，上传槽 2；队列各 8，各等 30s。上传结束就释放上传槽，等待上游 headers 和长 SSE 不占上传槽。增量校验器随上传运行，已无旧 worker 队列。
- wire/decoded 仍各 64MiB；spool 单份 normalized，最多预留 537,395,200 bytes（512.5MiB **磁盘**），不是内存。文件 0600、目录 0700。producer 与全部上传 FD 关闭后再清理。
- 上传 idle 30s（自身背压暂停期间不计）、接收 total 300s；每次上游上传 300s，完成后 headers 180s；SSE idle 300s。early headers 不会撤掉尚未结束的上传计时。取消会停止排队、producer、reader 和上游，并释放资源。
- `previous_response_id` 不做静默丢弃，明确本地 400，提示发送所需完整 input history；原因是网关无法恢复客户端没有上传的历史。HTTP `session-id`、`conversation_id`、item `id`、工具 `call_id` 保留。历史证据见 [ID 与缓存核对](id-cache-history-2026-11-01.md)。
- `gateway/deploy-update.sh`、`deploy-remote.py`：主源码和 streaming module 一起校验 SHA、备份、部署及回滚；更新阶段限额，移除旧 worker 参数，保留真实凭据与网络配置。
- `gateway/siwc-gateway-v2.test.mjs`、`streaming-json.test.mjs` 与 `package.json`：51 项 loopback/JSON 测试；覆盖提前转发、压缩、身份刷新、early 401、大小/语法/codec 尾部、分阶段队列、取消及 early 200/400、上游不读正文等。
- `gateway/linux-cgroup-benchmark.mjs`：顺序证明提前转发、20/30/50MiB、两路50MiB、两路SSE挂起时继续上传、取消与资源回零。
- `gateway/public-upload-probe.mjs`、`public-slow-upload-probe.py`：适配增量校验。合成 JSON 最后加非法尾部，保证完整上传才返回预期 400，且不提交完整模型请求；curl 核对实际上传字节。真实短推理需明确开启开关。
- `gateway/README.md`、env 示例和本记录：更新协议、容量、阶段超时、部署与验证边界。

## 已运行验证

- DSH 插件 41/41，构建通过。
- 本机 Node 24.18.0：51/51，约 6.84s。
- VM Node 22.22.0：51/51，约 29.05s。主源码和 module SHA 与候选一致。
- VM 独立测试 service 经 Caddy 2.11.7，MemoryMax=512MiB、swap=0，所有 10 个样本 HTTP200 / 完整 SSE / SHA 内容一致；其中有两路50MiB与两路长SSE挂起时的额外50MiB。cgroup峰值 **245,092,352 bytes（约233.74MiB）**，max/oom/oom_kill全部0。
- 顺序断言：模拟上游先收到 prefix，客户端才发送 suffix；不是按耗时猜测是否流式。
- SSE 挂起时，active_streams=2、inflight=2，但 upload/validator/upload FD/spool全部0；新50MiB正常完成。最终所有阶段与文件资源为0，SSE取消传到上游。
- 这些是合成 loopback 传输/网关资源验证，不是50MiB真实模型上下文验收。

证据：`docs/evidence/gateway-2.3.0-node24-tests.txt`、`gateway-2.3.0-node22-tests.tap`、`gateway-2.3.0-cgroup.json`。

## 正式部署

已部署版本2.3.0，主源码 SHA256 `7d71fa76a83cd544fe18553138565688e045a07415952c195e942b1e18cc4398`，streaming module SHA256 `751eb942efb674f522550ba293dba1ec22200a3f964351c2ad4ac8f3897ec666`。

VM备份：`/opt/siwc-gateway/backups/20261031T203054Z-30495/`。备份包含私有 env，保留在VM，未入库。Caddy无需修改/重启；只重启SIWC gateway，未重启DSH/Codex或修改客户端provider。

公网入口使用现有本机代理与正式 HTTPS：

- Node 固定长度 identity 20MiB，约40.67s上传完并收到预期尾部校验400；gzip 30MiB、zstd 50MiB分别约2.14s / 2.33s，均收到预期400。合成请求的非法尾部阻止完整JSON提交，不代表大上下文推理通过。这些取消会增加网关 upstream error 计数，属于本次探针的预期行为。
- 独立curl以512KiB/s上传20MiB，实际上传20,971,520 bytes，约40.12s，预期400，exit0。
- 模型目录200、11个模型、fresh；真实 `gpt-6.1-sol` / low 短请求200，约3.87s，收到 `response.completed`，输出精确为 `GATEWAY_V230_OK`。这项确认真实推理，非单纯health或HTTP200。
- 验证结果只覆盖本次样本，没有长期网络/晚高峰或大上下文模型可接受性保证。未接入CF/WARP，也未切换本机代理节点。

证据另见 `gateway-2.3.0-deployment.json`、`gateway-2.3.0-public.json`、`gateway-2.3.0-curl-upload.json`、`gateway-2.3.0-production.json`。

正式服务最终读回：主源码与 module SHA 一致；active/running，NRestarts=0，MemoryPeak=143,327,232 bytes，OOM=0。inflight、各队列、upload/validator/FD、spool、SSE均为0，spool目录为空；临时监听与测试文件已清理。后续部署以本仓库 `gateway/deploy-update.sh` 为入口，旧 default-workspace 脚本不作为2.3的部署入口。

后续已补DSH桌面多轮实景验收，并修复插件文本/工具块编号冲突造成的首轮提前结束。该修复更新客户端插件源码及lib，VM 2.3.0源码未改。修复前后证据及桌面测试见 [DSH实景验收](dsh-realistic-acceptance-2026-11-01.md)。
