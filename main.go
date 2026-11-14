// wifispeed — Wi-Fi/宽带测速 CLI + Web 可视化面板
//
// 方法论取自 GitHub 高星开源方案：
//   - 服务器发现与协议: sivel/speedtest-cli (★14k, speedtest.net 协议)
//   - 多连接并行测量:  librespeed/speedtest  (★15.3k, 打满高带宽的关键)
//   - HTML5 可视化:    librespeed/speedtest  (★15.3k, 内嵌 Web 仪表盘)
//   - 平台链路诊断:    macOS system_profiler
//   - CDN 多连接兜底:   speedtest 节点过慢时自动切换 Range 分块下载
//
// 零第三方依赖，仅 Go 标准库。

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"
)

// logFilter 丢弃 net/http 对空闲连接噪声数据的告警：部分 speedtest 节点在响应结束后
// 仍多发几个字节，Go 会打印 "Unsolicited response received on idle HTTP channel"。
// 这些字节不计入测量，告警无害但会刷屏；其余日志照常输出。
type logFilter struct{ w io.Writer }

func (f logFilter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("Unsolicited response received on idle HTTP channel")) {
		return len(p), nil
	}
	return f.w.Write(p)
}

var (
	cBold, cDim, cCyan, cGreen, cYellow, cRed, cReset string
)

func isTTY(f *os.File) bool {
	st, err := f.Stat()
	return err == nil && st.Mode()&os.ModeCharDevice != 0
}

func setupColors() {
	if isTTY(os.Stdout) && os.Getenv("NO_COLOR") == "" {
		cBold, cDim, cCyan, cGreen, cYellow, cRed, cReset =
			"\033[1m", "\033[2m", "\033[36m", "\033[32m", "\033[33m", "\033[31m", "\033[0m"
	}
}

// event 测试过程事件（CLI 打印 / Web NDJSON 流共用）
type event map[string]any

// Report 最终报告（-json 输出结构）
type Report struct {
	Time         string        `json:"time"`
	WiFi         *WiFiLink     `json:"wifi,omitempty"`
	Server       *SpeedServer  `json:"server,omitempty"`
	UploadServer *SpeedServer  `json:"upload_server,omitempty"`
	Latency      *LatencyStats `json:"latency,omitempty"`
	Download     *PhaseResult  `json:"download,omitempty"`
	Upload       *PhaseResult  `json:"upload,omitempty"`
}

// config 测试参数
type config struct {
	dlConc, upConc, warmSec, measureSec, chunkMB int
	serverURL                                    string
	noDown, noUp, noCDN, wifiOnly                bool
}

// normalized 收敛非法参数，避免越界/除零/超大分配
func (c config) normalized() config {
	const maxChunkMB = 64
	if c.dlConc < 1 {
		c.dlConc = 1
	}
	if c.upConc < 1 {
		c.upConc = 1
	}
	if c.warmSec < 0 {
		c.warmSec = 0
	}
	if c.measureSec < 1 {
		c.measureSec = 1
	}
	if c.chunkMB < 1 {
		c.chunkMB = 1
	}
	if c.chunkMB > maxChunkMB {
		c.chunkMB = maxChunkMB
	}
	return c
}

// runTest 完整测试流程，通过 ev 发出过程事件，返回最终报告。
// ctx 取消（关闭面板 / Ctrl+C）会中止在途阶段。
func runTest(ctx context.Context, cfg config, ev func(event)) *Report {
	report := Report{Time: time.Now().Format(time.RFC3339)}
	// 进度回调在 reporter goroutine 里执行，阶段名用原子值传递，避免数据竞争
	var curPhase atomic.Value
	curPhase.Store("")
	progress := func(mbps float64) {
		ev(event{"type": "progress", "phase": curPhase.Load().(string), "mbps": mbps})
	}
	setPhase := func(p string) { curPhase.Store(p) }
	canceled := func() bool { return ctx.Err() != nil }

	ev(event{"type": "banner"})

	// ── Wi-Fi 链路 ──
	if link, err := readWiFiLink(); err == nil {
		report.WiFi = link
		ev(event{"type": "wifi", "wifi": link})
	} else {
		ev(event{"type": "wifi-error", "error": err.Error()})
	}
	if cfg.wifiOnly {
		ev(event{"type": "done", "report": report})
		return &report
	}

	// ── 服务器择优（并行探测延迟） ──
	var (
		server     SpeedServer
		haveServer bool
		ranked     []serverProbe
	)
	if cfg.serverURL != "" {
		base := cfg.serverURL
		if !endsWithSlash(base) {
			base += "/"
		}
		server = SpeedServer{URL: base + "upload.php", Host: base, Name: "自定义", Sponsor: "manual"}
		haveServer = true
	} else {
		ev(event{"type": "status", "tag": "服务器", "text": "并行探测最近节点…"})
		if list, err := discoverServers(30); err != nil {
			ev(event{"type": "notice", "text": err.Error() + "，转入 CDN 模式（仅下载测速）"})
		} else {
			ranked = probeServers(ctx, list)
			if len(ranked) > 0 {
				server, haveServer = ranked[0].server, true
			} else {
				ev(event{"type": "notice", "text": "所有节点探测失败，转入 CDN 模式（仅下载测速）"})
			}
		}
	}
	if canceled() {
		ev(event{"type": "done", "report": report})
		return &report
	}

	warm := time.Duration(cfg.warmSec) * time.Second
	measure := time.Duration(cfg.measureSec) * time.Second
	cdnConc := cfg.dlConc * 2 // CDN 兜底并发：至少 32 才能压满千兆
	if cdnConc < 32 {
		cdnConc = 32
	}

	// ── 延迟 ──
	if haveServer {
		report.Server = &server
		ev(event{"type": "server", "text": server.String()})
		ev(event{"type": "status", "tag": "延迟", "text": "探测中…"})
		if lat, err := measureLatency(ctx, server.Base(), 10); err != nil {
			if !canceled() {
				ev(event{"type": "error", "text": "延迟测试失败: " + err.Error()})
			}
		} else {
			report.Latency = lat
			ev(event{"type": "latency", "stats": lat})
		}
	}

	// ── 下载 ──
	stDownMbps := 0.0 // speedtest 节点自身的下行（供上传跳过提示引用）
	if !cfg.noDown && !canceled() {
		if haveServer {
			setPhase("download")
			ev(event{"type": "phase", "phase": "download", "text": fmt.Sprintf("speedtest 节点 · %d 并发 · 预热 %ds + 计量 %ds", cfg.dlConc, cfg.warmSec, cfg.measureSec)})
			res, err := runDownloadPhase(ctx, server.Base(), cfg.dlConc, warm, measure, progress)
			if err != nil {
				if !canceled() {
					ev(event{"type": "error", "text": "下载测试失败: " + err.Error()})
				}
			} else {
				report.Download = res
				stDownMbps = res.Mbps
				ev(event{"type": "result", "phase": "download", "result": res})
			}
			// speedtest 节点过慢 → CDN 多连接兜底复测
			if report.Download != nil && report.Download.Mbps < 100 && !cfg.noCDN && !canceled() {
				ev(event{"type": "notice", "text": fmt.Sprintf("speedtest 节点仅 %.1f Mbps，切换 CDN 多连接复测（%d 并发）", report.Download.Mbps, cdnConc)})
				setPhase("download")
				res2, err2 := runCDNDownloadPhase(ctx, cdnConc, warm, measure, progress)
				if err2 == nil && res2.Mbps > report.Download.Mbps {
					report.Download = res2
					ev(event{"type": "result", "phase": "download", "result": res2})
				} else if err2 != nil && !canceled() {
					ev(event{"type": "error", "text": "CDN 复测失败: " + err2.Error()})
				}
			}
		} else { // cdnOnly
			setPhase("download")
			ev(event{"type": "phase", "phase": "download", "text": fmt.Sprintf("CDN 模式 · %d 并发 · 预热 %ds + 计量 %ds", cdnConc, cfg.warmSec, cfg.measureSec)})
			res, err := runCDNDownloadPhase(ctx, cdnConc, warm, measure, progress)
			if err != nil {
				if !canceled() {
					ev(event{"type": "error", "text": "下载测试失败: " + err.Error()})
				}
			} else {
				report.Download = res
				ev(event{"type": "result", "phase": "download", "result": res})
			}
		}
	}

	// ── 上传 ──
	if !cfg.noUp && !canceled() {
		// 节点下行能力不足（或已触发 CDN 兜底）时，上传结果必然失真；跳过并说明，除非显式 -server
		nodeWeak := report.Download != nil &&
			(report.Download.Source == "CDN" || report.Download.Mbps < 20)
		switch {
		case !haveServer:
			ev(event{"type": "notice", "text": "CDN 模式无上传端点，跳过"})
		case nodeWeak && cfg.serverURL == "":
			ev(event{"type": "notice", "text": fmt.Sprintf("speedtest 节点能力不足（其下行仅 %.1f Mbps），上传结果会失真，跳过", stDownMbps)})
		default:
			runUpload(ctx, cfg, server, ranked, warm, measure, ev, &report, setPhase, progress)
		}
	}

	// ── 千兆达成度 ──
	if report.Download != nil {
		pct := report.Download.Mbps / 1000 * 100
		verdict := ""
		switch {
		case pct >= 85:
			verdict = "千兆基本跑满 ✓"
		case pct >= 50:
			verdict = "未跑满千兆：受 Wi-Fi 链路/路由器/测速节点限制"
		default:
			verdict = "明显低于千兆，建议排查 Wi-Fi 链路或换节点复测"
		}
		ev(event{"type": "verdict", "pct": pct, "text": verdict})
	}
	ev(event{"type": "done", "report": report})
	return &report
}

// runUpload 上传阶段：先探测哪些节点真正接受上传，失败再换下一个节点。
// speedtest 网络里存在「延迟极低但上传连接被重置」的节点（如部分 Chief Telecom 节点），
// 只按延迟选节点会让上传稳定失败。
func runUpload(ctx context.Context, cfg config, selected SpeedServer, ranked []serverProbe,
	warm, measure time.Duration, ev func(event), report *Report,
	setPhase func(string), progress func(float64)) {

	candidates := []SpeedServer{selected}
	if cfg.serverURL == "" {
		ev(event{"type": "status", "tag": "上传", "text": "探测可上传节点…"})
		candidates = uploadCandidates(ctx, ranked)
		if len(candidates) == 0 {
			ev(event{"type": "notice", "text": "附近节点都拒绝上传请求，跳过上传测试"})
			return
		}
	}

	attempts := 0
	for _, srv := range candidates {
		if attempts >= maxUploadAttempts || ctx.Err() != nil {
			return
		}
		attempts++
		setPhase("upload")
		if srv.Host != selected.Host {
			ev(event{"type": "notice", "text": "上传改用节点: " + srv.String()})
		}
		ev(event{"type": "phase", "phase": "upload", "text": fmt.Sprintf("%d 并发 · 块 %dMB · 预热 %ds + 计量 %ds", cfg.upConc, cfg.chunkMB, cfg.warmSec, cfg.measureSec)})

		res, err := runUploadPhase(ctx, srv.URL, cfg.upConc, warm, measure, cfg.chunkMB, progress)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			ev(event{"type": "error", "text": "上传测试失败: " + err.Error()})
			continue
		}
		used := srv
		report.Upload = res
		report.UploadServer = &used
		ev(event{"type": "result", "phase": "upload", "result": res})
		return
	}
}

// cliEmitter 把事件流渲染成终端输出
func cliEmitter(human *os.File) func(event) {
	tty := human == os.Stdout && isTTY(os.Stdout)
	p := func(format string, a ...any) { fmt.Fprintf(human, format+"\n", a...) }
	lastLine := time.Now()
	lastWasProgress := false
	before := func() { // 进度行后先换行
		if lastWasProgress {
			lastWasProgress = false
			if tty {
				fmt.Fprintln(os.Stdout)
			}
		}
	}
	return func(ev event) {
		switch ev["type"] {
		case "banner":
			p("%s═══ wifispeed — Wi-Fi / 宽带测速 ═══%s", cBold, cReset)
		case "wifi":
			before()
			link := ev["wifi"].(*WiFiLink)
			p("%s[Wi-Fi]%s %s", cCyan, cReset, link.DisplaySSID())
			p("  %-8s %s · %s", "链路", link.PHY, link.Channel)
			p("  %-8s %d dBm / 噪声 %d dBm → SNR %d dB（%s）",
				"信号", link.Signal, link.Noise, link.SNR, link.Quality())
		case "wifi-error":
			before()
			p("%s[Wi-Fi]%s %v", cYellow, cReset, ev["error"])
		case "status":
			before()
			p("%s[%s]%s %s", cCyan, ev["tag"], cReset, ev["text"])
		case "server":
			before()
			p("%s[服务器]%s %s", cCyan, cReset, ev["text"])
		case "latency":
			before()
			lat := ev["stats"].(*LatencyStats)
			p("  平均 %s%.1f ms%s · 最小 %.1f ms · 抖动 %.1f ms · 丢包 %.0f%%",
				cGreen, lat.AvgMs, cReset, lat.MinMs, lat.JitterMs, lat.LossPct)
		case "phase":
			before()
			p("%s[%s]%s %s", cCyan, phaseName(ev["phase"].(string)), cReset, ev["text"])
		case "progress":
			mbps := ev["mbps"].(float64)
			if tty {
				fmt.Fprintf(os.Stdout, "\r%s%-8s%s %7.1f Mbps   ", cDim, "进行中", cReset, mbps)
				lastWasProgress = true
			} else if time.Since(lastLine) >= 2500*time.Millisecond {
				p("  … %7.1f Mbps", mbps)
				lastLine = time.Now()
			}
		case "result":
			before()
			res := ev["result"].(*PhaseResult)
			p("  %s%.1f Mbps%s （峰值 %.1f · %.1f MB/%.1fs · %d 错误 · %s）",
				cGreen, res.Mbps, cReset, res.PeakMbps, res.MB, res.Seconds, res.Errors, res.Source)
		case "notice":
			before()
			p("%s⚠ %s%s", cYellow, ev["text"], cReset)
		case "error":
			before()
			p("%s✗ %s%s", cRed, ev["text"], cReset)
		case "verdict":
			before()
			p("%s[结论]%s 下行达成千兆的 %s%.0f%%%s — %s",
				cBold, cReset, cCyan, ev["pct"], cReset, ev["text"])
		case "done":
			// 终端无需额外输出
		}
	}
}

func phaseName(p string) string {
	if p == "download" {
		return "下载"
	}
	return "上传"
}

func main() {
	var (
		dlConc     = flag.Int("c", 16, "下载并发连接数")
		upConc     = flag.Int("uc", 8, "上传并发连接数")
		warmSec    = flag.Int("w", 3, "预热秒数（不计入结果）")
		measureSec = flag.Int("t", 10, "计量秒数")
		chunkMB    = flag.Int("chunk", 8, "上传块大小 MB（节点拒绝大请求体时自动降为 2/1）")
		serverURL  = flag.String("server", "", "指定服务器 base URL（跳过自动发现）")
		jsonOut    = flag.Bool("json", false, "以 JSON 输出结果")
		noDown     = flag.Bool("no-download", false, "跳过下载测试")
		noUp       = flag.Bool("no-upload", false, "跳过上传测试")
		noCDN      = flag.Bool("no-cdn-fallback", false, "禁用 CDN 多连接兜底")
		wifiOnly   = flag.Bool("wifi-only", false, "仅显示 Wi-Fi 链路信息")
		web        = flag.Bool("web", false, "启动 Web 可视化面板")
		port       = flag.Int("port", 8187, "Web 面板端口")
	)
	flag.Parse()
	setupColors()
	log.SetOutput(logFilter{w: os.Stderr})

	cfg := config{
		dlConc: *dlConc, upConc: *upConc, warmSec: *warmSec, measureSec: *measureSec,
		chunkMB: *chunkMB, serverURL: *serverURL,
		noDown: *noDown, noUp: *noUp, noCDN: *noCDN, wifiOnly: *wifiOnly,
	}.normalized()

	if *web {
		addr := fmt.Sprintf("127.0.0.1:%d", *port)
		url := "http://" + addr + "/"
		fmt.Printf("wifispeed 可视化面板: %s （Ctrl+C 退出）\n", url)
		// WIFISPEED_NO_OPEN=1 用于无头/预览环境，避免抢占默认浏览器
		if os.Getenv("WIFISPEED_NO_OPEN") == "" {
			openBrowser(url)
		}
		if err := serveWeb(addr, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "Web 服务退出: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// CLI 路径：-json 时人读输出走 stderr，stdout 只留 JSON
	human := os.Stdout
	if *jsonOut {
		human = os.Stderr
	}
	report := runTest(context.Background(), cfg, cliEmitter(human))
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.SetEscapeHTML(false)
		enc.Encode(report)
		return
	}
	_ = report
}

func endsWithSlash(s string) bool { return len(s) > 0 && s[len(s)-1] == '/' }
