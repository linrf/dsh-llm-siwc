# SIWC VM gateway

`siwc-gateway-v2.mjs` 和 `streaming-json.mjs` 是网关 2.3.0 源码。VM 分别运行 `/opt/siwc-gateway/siwc-gateway.mjs` 与同目录的 `streaming-json.mjs`。本目录不保存 bearer、OAuth、SSH key、订阅或真实请求正文。

## 请求与资源约定

```text
客户端 → wire 限额 → gzip/zstd 解码 → decoded 限额 → 增量 JSON 校验/顶层改写
                                                        ↓
                                                 私有重试文件
                                                        ↓ 持续读取已写入部分
                                                     上游请求
上游 SSE → 网关（含背压）→ 客户端
```

- 不再完整读取或 `JSON.parse` 整个正文，不再启动大 JSON 子进程。保留字段按原始 JSON 字节转发，包括数字、转义、消息 `id` 与工具 `call_id`。只改 SIWC 要求的顶层字段，强制 `store:false`、`stream:true`。
- 接收、增量处理、写盘和上游上传重叠。重试文件不是等待完整接收的前提；它保存已写入的数据，上传端可持续读取。最后一个 `}` 必须等 JSON、压缩完整性和大小校验全通过后才发送，错误正文不会提交成完整请求。上游可提前看到部分正文；不再保证非法请求从未建立上游连接。
- Responses 支持 identity、gzip、zstd。wire 与 decoded 分别限制 64MiB，zstd window 上限 8MiB；不支持编码为 415，坏压缩/JSON 为 400，超限为 413。Node zstd 只处理首 frame，因此多 frame 或尾部数据明确拒绝。
- SIWC HTTP 不支持 `previous_response_id`。网关明确返回 400，要求省略并在 `input` 中发送所需完整历史；不会静默删除后假定上下文齐全。稳定的 HTTP `session-id` / `conversation_id` 请求头保留，401 重放同样保留。原因与历史证据见 [ID 与缓存核对](../docs/id-cache-history-2026-11-01.md)。
- 全请求/模型响应槽为 **8**，含长 SSE；上传槽为 **2**，只覆盖正文接收及上游上传。上游等待 headers 与长 SSE 不占上传槽。两类队列各最多 **8**，各排队最多 **30s**。增量 JSON 校验随上传进行，单独统计 active，无旧 worker 队列。
- 每个请求预留一份 normalized 文件上限，默认全部预留约 **512.5MiB 磁盘**。目录 0700、文件 0600，必须放真实磁盘；当前 VM 的 `/tmp` 为 tmpfs。磁盘不足或预留额占满时受控拒绝。启动清理已退出进程留下的受控目录；spool 目录只供一个网关主进程使用。
- 上传停滞 **30s**、接收总时限 **300s**；网关自身背压暂停接收时不计算客户端停滞。每次上游上传最多 **300s**，上传结束后等待 headers 最多 **180s**，SSE idle **300s**。提前收到 headers 不会关闭未结束的上传计时。非成功上游响应体最多 **64KiB / 10s**。
- 上游 401 只重试一次。先关闭该次上传读取，保留接收和写盘；完整校验成功后刷新令牌，并重新读文件发送。OAuth 刷新仍为单 owner / singleflight。不自动重放其他错误或已经输出的 SSE。
- 客户端取消会停止排队、接收、解码、校验和上游。只有 producer 与所有上传文件描述符关闭后，才删除文件、释放预留。正常正文上传成功后清理文件，不为长 SSE 保留正文。

`/metrics` 只在 VM loopback 暴露。检查 inflight、queued、upload_active/queued、normalizer_active、upstream_uploads_active、spool、RSS、wire/decoded/normalized bytes 与上游错误。`normalizer_queued` 和旧 `normalizer_peak_rss_bytes` 为兼容指标，2.3 起恒为 0。日志只记体积、状态和阶段耗时，不记正文。`/healthz` 表示进程存活，不是上游模型可用性探测。

## 测试

仓库根目录运行 `npm run test:gateway`。HTTP 与增量 JSON 测试均使用 loopback 模拟上游和假凭据，不调用真实模型；DSH 插件使用 `npm test`、`npm run build`。

`linux-cgroup-benchmark.mjs` 用于已有 Linux/systemd VM，需 root、siwc-gw 用户、Node 22.22.0 和真实磁盘 `/var/lib/siwc-gateway`。候选源码和 `streaming-json.mjs` 放同目录且 siwc-gw 可读。生成器、模拟上游位于临时网关 cgroup 外：

```sh
node gateway/linux-cgroup-benchmark.mjs /absolute/path/to/siwc-gateway-v2.mjs
```

固定 MemoryMax=512MiB、swap=0，检查上游先收到 prefix 再发送客户端 suffix、20/30/50MiB、两路 50MiB、挂起两路 SSE 后仍可上传、取消传播和资源回零。可设置 BENCH_INGRESS_PORT 加入已有 loopback Caddy。Caddy 不设负 flush_interval，默认 SSE 即时输出并保留取消。样本通过不等于任意 JSON、任意模型上下文或长期负载均已验收。

## 增量部署

env、service、Caddyfile 模板不含凭据，真实配置只在 VM。

`deploy-update.sh` 同时上传主源码与 streaming 模块，验证两个 SHA，检查无活动请求并备份旧源码/模块/配置，再更新分阶段 env、重启服务、核对版本。健康检查失败恢复全部文件及原权限，并核对旧版本。活动检查是快照，不保证零停机迁移；发现晚到的活动请求会恢复文件且不重启。它不改 OAuth、SSH、防火墙或系统网络。

```sh
SIWC_SSH_TARGET=root@gateway-host \
SIWC_SSH_IDENTITY=/private/path/key \
SIWC_KNOWN_HOSTS=/private/path/known_hosts \
bash gateway/deploy-update.sh
```

VM 备份 `/opt/siwc-gateway/backups/<UTC timestamp>-<PID>/` 包含私有 env，不能入库。2.2 历史验证见 [原记录](../docs/changes-2026-11-01.md)，2.3 见 [流式改造记录](../docs/streaming-2026-11-01.md)。

`public-upload-probe.mjs` 使用 SIWC_BASE_URL、SIWC_GATEWAY_TOKEN 和可选 SIWC_PROXY_URL，上传带非法尾部的合成 JSON（20MiB 慢上传、gzip 30MiB、zstd 50MiB），应在收到完整正文后返回 400；末尾 `}` 不会提交上游。`public-slow-upload-probe.py` 另用 curl 固定长度 512KiB/s 上传 20MiB，并核对实际上传字节。完整上传还应结合 VM byte 日志核对。仅显式设置 SIWC_MODEL_SMOKE=1 才额外执行真实短请求。大体积传输测试不代表大上下文已完成真实模型推理。
