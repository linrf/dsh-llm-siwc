# SIWC VM gateway

`siwc-gateway-v2.mjs` 是源码，版本由文件内 `VERSION` 标识。VM 运行副本是 `/opt/siwc-gateway/siwc-gateway.mjs`。本目录不保存 bearer、OAuth、SSH key、订阅或真实请求正文。

## 请求与资源约定

- Responses 支持 identity、gzip、zstd。wire 与 decoded body 分别限制为 64MiB，zstd window 上限 8MiB。不支持的编码返回 415，坏压缩返回 400，超限返回 413。当前 Node zstd 只处理首 frame，因此多 frame 或尾部数据明确返回 400。
- 上传流式解码到真实磁盘，文件 0600、目录 0700。最多两个 Responses 请求同时运行，队列最多八个。每个请求预留输入及输出磁盘容量，总额约 256MiB；磁盘不足或容量占满时拒绝进入。
- JSON 在单个隔离 Node 子进程处理，heap 上限 224MiB。分块写出规范化 JSON 后子进程退出。主进程以文件流发送上游，401 重试重新打开文件；正常上传的文件描述符关闭后删除文件，不为长 SSE 保留已上传的正文；上游提前返回时先完整消费响应，再取消剩余上传并关闭文件。过深、过复杂或超过处理预算的 JSON 会受控拒绝；64MiB 不是任意 JSON 结构都能成功处理的保证。
- 上传停滞 30s、上传总时限 300s、排队 30s、JSON 处理 30s；上游 headers 180s、SSE idle 300s；非成功上游响应体最多读 64KiB、等待 10s。队列与上传分别计时。客户端断开会取消队列、上传、子进程或上游流。
- 专用 spool 目录只供一个 gateway 主进程使用。启动时清理已退出进程留下的受控请求目录。service 禁止 core dump，并在停止时回收子进程。

VM 验收固定 Node.js 22.22.0、MemoryMax=512MiB、swap=0。50MiB 大字符串和两路并发含中文大字符串属于已验收样本，不能推断任意 Node 版本、任意 JSON 或长期负载的峰值。

`/metrics` 只在 VM loopback 暴露。检查 inflight、queued、normalizer、spool、RSS、wire/decoded/normalized bytes 和上游错误。日志只记体积及阶段耗时，不记正文。`/healthz` 表示进程存活，不是上游可用性探测。

## 测试

在仓库根目录运行 `npm run test:gateway`。HTTP 测试只用 loopback 模拟上游和假凭据，不调用真实模型。

`linux-cgroup-benchmark.mjs` 用于已有 Linux/systemd VM，需 root、siwc-gw 用户和真实磁盘 `/var/lib/siwc-gateway`。它将网关放进 512MiB、禁用 swap 的临时 service；生成器和模拟上游位于 service 外。运行方式：

```sh
node gateway/linux-cgroup-benchmark.mjs /absolute/path/to/siwc-gateway-v2.mjs
```

可先建立 loopback Caddy 入口，再设置 BENCH_INGRESS_PORT 验证代理。Caddy 不设负的 flush_interval：SSE 默认即时输出，取消能继续传到网关。基准结束会停止临时网关并清理假凭据和请求文件。

## 增量部署

env、service、Caddyfile 模板均不含凭据。真实配置只放 VM。spool 必须用真实磁盘；当前 VM 的 /tmp 是 tmpfs。

deploy-update.sh 用于已存在的网关：上传源码、校验 SHA、确认无活动请求、备份旧源码与配置，仅更新大请求 env 和 service drop-in，移除 Caddy 负 flush_interval，再重启并核对版本。活动检查是快照，部署应在维护窗口执行，不提供零停机迁移保证；发现晚到的活动请求会恢复文件，不重启当前服务。健康检查失败则恢复旧文件及原权限，并核对旧版本。它不改 OAuth、SSH、防火墙或系统网络。

```sh
SIWC_SSH_TARGET=root@gateway-host \
SIWC_SSH_IDENTITY=/private/path/key \
SIWC_KNOWN_HOSTS=/private/path/known_hosts \
bash gateway/deploy-update.sh
```

VM 备份位于 `/opt/siwc-gateway/backups/<UTC timestamp>-<PID>/`，包含私有 env，不能入库。源码 SHA、结果和验证范围见 [改造记录](../docs/changes-2026-11-01.md)。

public-upload-probe.mjs 从环境变量读取 SIWC_BASE_URL、SIWC_GATEWAY_TOKEN 和可选 SIWC_PROXY_URL，检查 20MiB 慢上传及 gzip / zstd；使用顶层数组，让网关在推理前受控返回 400。只有显式设置 SIWC_MODEL_SMOKE=1 才会额外执行一次真实短请求。

public-slow-upload-probe.py 使用相同环境变量，以 curl 固定长度、512KiB/s 上传 20MiB，独立检查 TLS / 上传路径。两种公网探针都会产生预期的 400 校验响应，不代表大输入已完成真实模型推理。失败不会自动重放模型请求。
