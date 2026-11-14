# wifispeed — Wi-Fi / 宽带测速 CLI

自研的 macOS Wi-Fi 测速工具，Go 标准库实现、零第三方依赖、单二进制。

源码：<https://github.com/luoqi951102/wifispeed> · License: MIT

## 隐私与安全

- 面板只监听 `127.0.0.1`（见 `main.go` 的 `serveWeb` 地址构造），默认不暴露到局域网/公网；不要用端口转发或反代把它开放出去。
- `/api/run` 只接受 POST，并校验 `Origin` / `Sec-Fetch-Site`：其他网页无法跨站偷偷触发本机测速（`sameOrigin`，有单测）。
- 无账号、无遥测、无第三方依赖；不上传 SSID、位置或测速结果。
- 上传测试发送的是随机字母数字（`content1=[a-zA-Z0-9]`），不含任何个人信息。
- `-server` 只应指向你信任的节点；测速会产生真实流量，请勿在计费/限速链路上反复跑。
- 仓库不包含密钥、内网地址、个人路径或实机截图；提交使用 GitHub noreply 邮箱。

## 方法论来源（GitHub 高星方案）

| 借鉴 | 仓库 | 用了什么 |
|---|---|---|
| sivel/speedtest-cli (★14k) | speedtest.net 协议 | 服务器发现（`api/js/servers`）、`latency.txt` 延迟探测、`random*.jpg` 下载、`upload.php` 上传 |
| librespeed/speedtest (★15.3k) | HTML5 自托管测速 | 多连接并行测量 + 预热/计量分窗（打满千兆的关键） |
| 自研 | — | CDN 多连接 Range 兜底、100ms 时间桶峰值统计、macOS 链路诊断 |

## 特性

- **并行节点择优**：并行探测最近 30 个 speedtest.net 节点，选延迟最低者（运营商节点优先）
- **多连接下载**：默认 16 并发，预热 3s 不计入 + 计量 10s，1s 滑窗峰值
- **CDN 兜底**：speedtest 节点 < 100 Mbps 时自动切 32 并发 Range 分块下载（国内 CDN，绕开节点限速）
- **上传节点探测**：先对候选节点做 256KB 上传探测，跳过「延迟低但拒绝/重置上传」的节点；失败自动换下一个节点
- **上传计量**：边发送边计字节（有实时进度与峰值），8MB→2MB→1MB 自适应块，仅统计 2xx
- **快速失败**：节点连续 5s 无数据往来、或连续快速拒绝上传，立即中止该阶段（不再空耗 3×13s）
- **硬截止 + 可取消**：每阶段 context 到期强制取消；关闭/刷新面板即中止测试，不会一直占用面板
- **Wi-Fi 诊断**：SSID / 信道频宽 / PHY / RSSI / SNR 及质量评级（支持中英文系统；SSID 被 macOS 隐藏时给出授权提示）
- **千兆达成度结论** + `-json` 机器可读输出（含实际使用的上传节点）

## 用法

```bash
go build -o wifispeed .
./wifispeed                 # 完整测试（Wi-Fi → 节点 → 延迟 → 下载 → 上传）
./wifispeed -wifi-only      # 只看 Wi-Fi 链路
./wifispeed -c 24 -t 15     # 24 并发、计量 15s
./wifispeed -json           # JSON 输出（进度与说明走 stderr）
./wifispeed -no-upload      # 跳过上传
./wifispeed -server http://host/speedtest/   # 指定节点（强制上传测试）
./wifispeed -no-cdn-fallback # 禁用 CDN 兜底，只信 speedtest 节点
./wifispeed -web -port 8187  # Web 面板
```

## 已知局限

- **上传请求体必须是字母数字**：部分 speedtest 节点（如 Chief Telecom 系列）会直接重置
  携带随机二进制 `content1=` 表单的 POST，表现为「142 次错误、0 有效请求」。本工具按
  speedtest-cli 的做法只用 `[a-zA-Z0-9]` 填充载荷（见 `buildUploadBody`）。
- 上传依赖 speedtest.net 节点可用性；附近节点全部拒绝上传时会跳过并提示，而不是给出 0 或者报错退出
- 测的是「Wi-Fi + 宽带」端到端速率；若要单独测 Wi-Fi 链路（排除宽带），需 LAN 内 iperf3 对测

## 测试

```bash
go test -race -cover ./...
```

覆盖测量核心（时间桶峰值、延迟统计、上传计量/降级/重定向拒绝、节点探测、Range 下载）与
Wi-Fi 解析；`main.go` 的编排逻辑通过本地 httptest 假节点做端到端覆盖。
