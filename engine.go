package main

// 测速引擎：speedtest.net 协议（服务器发现/延迟/下载/上传）+ LibreSpeed 式多连接并行测量。
// 协议参考: github.com/sivel/speedtest-cli (★14k) 与 librespeed/speedtest (★15.3k) 的方法论。

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// userAgent speedtest.net 节点会按 UA 做基本过滤，Go 默认 UA 容易被拒
	userAgent = "wifispeed/1.1 (speedtest.net protocol)"

	latencyFile  = "latency.txt"
	downloadFile = "random4000x4000.jpg"

	// stallTimeout 阶段内连续这么久没有任何数据往来，即判定节点不可用（快速失败）
	stallTimeout = 5 * time.Second
	// phaseGrace 硬截止后留给在途请求的收尾时间
	phaseGrace = 2 * time.Second
	// retryDelay 失败后的退避，避免打满错误循环
	retryDelay = 300 * time.Millisecond

	// 上传节点探测：speedtest 网络里「延迟低但拒绝/重置上传」的节点很常见
	uploadProbeBytes   = 256 << 10
	uploadProbeTimeout = 5 * time.Second
	maxUploadProbes    = 6
	// 完整上传阶段最多尝试的节点数（每多试一个节点多花 预热+计量 秒）
	maxUploadAttempts = 2

	// CDN 多连接下载兜底
	cdnChunk = 16 << 20
)

// cdnFileURL CDN 兜底文件（变量以便测试替换）
var cdnFileURL = "https://dldir1.qq.com/weixin/Windows/WeChatSetup.exe"

// errCanceled 上层主动取消（关闭面板 / Ctrl+C）
var errCanceled = errors.New("测试已取消")

// SpeedServer speedtest.net 服务器条目（URL 为完整 upload.php 地址）
type SpeedServer struct {
	URL      string  `json:"url"`
	Name     string  `json:"name"`
	Sponsor  string  `json:"sponsor"`
	Country  string  `json:"country"`
	Distance float64 `json:"d"`
	Host     string  `json:"host"`
}

// Base 去掉 upload.php 得到服务器基础路径
func (s SpeedServer) Base() string { return strings.TrimSuffix(s.URL, "upload.php") }

func (s SpeedServer) String() string {
	loc := s.Name
	if s.Country != "" {
		loc += ", " + s.Country
	}
	return fmt.Sprintf("%s (%s) %s", loc, s.Sponsor, s.Host)
}

// discoverServers 按距离发现最近的 speedtest.net 服务器
func discoverServers(limit int) ([]SpeedServer, error) {
	apiUrls := []string{
		"https://www.speedtest.net/api/js/servers?engine=js&limit=30",
		"http://www.speedtest.net/api/js/servers?engine=js&limit=30",
	}
	for _, u := range apiUrls {
		cli := &http.Client{Timeout: 12 * time.Second}
		resp, err := cli.Get(u)
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		var list []SpeedServer
		if json.Unmarshal(b, &list) != nil {
			continue
		}
		clean := make([]SpeedServer, 0, len(list))
		for _, s := range list {
			// 过滤脏数据：没有 upload.php 的条目无法用于上传/下载测量
			if s.Host == "" || !strings.HasPrefix(s.URL, "http") || !strings.Contains(s.URL, "upload.php") {
				continue
			}
			clean = append(clean, s)
		}
		if len(clean) == 0 {
			continue
		}
		sort.SliceStable(clean, func(i, j int) bool { return clean[i].Distance < clean[j].Distance })
		if len(clean) > limit {
			clean = clean[:limit]
		}
		return clean, nil
	}
	return nil, errors.New("无法获取 speedtest.net 服务器列表，可用 -server 指定 base URL")
}

// LatencyStats 延迟统计（毫秒）
type LatencyStats struct {
	MinMs    float64 `json:"min_ms"`
	AvgMs    float64 `json:"avg_ms"`
	JitterMs float64 `json:"jitter_ms"`
	Samples  int     `json:"samples"`
	LossPct  float64 `json:"loss_pct"`
}

// measureLatency 用 latency.txt 做 N 次小请求测 RTT 与抖动
func measureLatency(ctx context.Context, base string, probes int) (*LatencyStats, error) {
	cli := &http.Client{Timeout: 4 * time.Second}
	ds := make([]float64, 0, probes)
	fails := 0
	for i := 0; i < probes; i++ {
		if ctx.Err() != nil {
			return nil, errCanceled
		}
		t0 := time.Now()
		req, err := newRequest(ctx, http.MethodGet, withCacheBust(base+latencyFile), nil)
		if err != nil {
			break
		}
		resp, err := cli.Do(req)
		if err != nil {
			fails++
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		ds = append(ds, msSince(t0))
		sleepCtx(ctx, 60*time.Millisecond)
	}
	if len(ds) < 3 {
		return nil, fmt.Errorf("延迟探测失败过多 (%d/%d)", fails, probes)
	}
	return buildLatencyStats(ds, fails, probes), nil
}

func buildLatencyStats(ds []float64, fails, probes int) *LatencyStats {
	minD, sum := ds[0], 0.0
	for _, d := range ds {
		if d < minD {
			minD = d
		}
		sum += d
	}
	jitterSum := 0.0
	for i := 1; i < len(ds); i++ {
		jitterSum += absF(ds[i] - ds[i-1])
	}
	jitter := 0.0
	if len(ds) > 1 {
		jitter = jitterSum / float64(len(ds)-1)
	}
	loss := 0.0
	if probes > 0 {
		loss = float64(fails) / float64(probes) * 100
	}
	return &LatencyStats{
		MinMs:    minD,
		AvgMs:    sum / float64(len(ds)),
		JitterMs: jitter,
		Samples:  len(ds),
		LossPct:  loss,
	}
}

func absF(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func msSince(t0 time.Time) float64 { return float64(time.Since(t0).Microseconds()) / 1000.0 }

// serverProbe 单个候选节点的延迟探测结果
type serverProbe struct {
	server SpeedServer
	stats  *LatencyStats
}

// quickLatency 2 次快探测，返回平均 RTT；节点不通时 ok=false
func quickLatency(ctx context.Context, base string) (float64, bool) {
	// 快探测：2 次 × 2.5s 超时，避免死节点拖慢整个择优
	cli := &http.Client{Timeout: 2500 * time.Millisecond}
	var sum float64
	n := 0
	for i := 0; i < 2; i++ {
		if ctx.Err() != nil {
			break
		}
		t0 := time.Now()
		req, err := newRequest(ctx, http.MethodGet, withCacheBust(base+latencyFile), nil)
		if err != nil {
			break
		}
		resp, err := cli.Do(req)
		if err != nil {
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		sum += msSince(t0)
		n++
	}
	if n == 0 {
		return 0, false
	}
	return sum / float64(n), true
}

// probeServers 并行探测候选节点延迟，返回按平均延迟升序的可达节点（运营商节点同延迟优先）
func probeServers(ctx context.Context, cands []SpeedServer) []serverProbe {
	ch := make(chan serverProbe, len(cands))
	var wg sync.WaitGroup
	for _, c := range cands {
		wg.Add(1)
		go func(s SpeedServer) {
			defer wg.Done()
			avg, ok := quickLatency(ctx, s.Base())
			if !ok {
				return
			}
			ch <- serverProbe{server: s, stats: &LatencyStats{AvgMs: avg, MinMs: avg, Samples: 2}}
		}(c)
	}
	wg.Wait()
	close(ch)

	out := make([]serverProbe, 0, len(cands))
	for r := range ch {
		out = append(out, r)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].stats.AvgMs != out[j].stats.AvgMs {
			return out[i].stats.AvgMs < out[j].stats.AvgMs
		}
		return isISP(out[i].server.Sponsor) && !isISP(out[j].server.Sponsor)
	})
	return out
}

func isISP(sponsor string) bool {
	l := strings.ToLower(sponsor)
	for _, kw := range []string{"电信", "联通", "移动", "telecom", "unicom", "mobile"} {
		if strings.Contains(l, kw) {
			return true
		}
	}
	return false
}

// PhaseResult 单阶段（下载/上传）测量结果
type PhaseResult struct {
	Mbps     float64 `json:"mbps"`
	PeakMbps float64 `json:"peak_mbps"`
	MB       float64 `json:"mb"`
	Seconds  float64 `json:"seconds"`
	Workers  int     `json:"workers"`
	Errors   int     `json:"errors"`
	Source   string  `json:"source,omitempty"`
}

// meter 按字节真实到达时间落入 100ms 桶，用于精确的 1s 滑窗峰值
// （避免“整块计数在请求完成时刻”造成的速率聚簇伪峰）
type meter struct {
	mu      sync.Mutex
	start   time.Time
	span    time.Duration
	buckets []int64
}

func newMeter(start time.Time) *meter {
	return &meter{start: start, span: 100 * time.Millisecond}
}

func (m *meter) add(n int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	idx := int(time.Since(m.start) / m.span)
	if idx < 0 {
		idx = 0
	}
	for len(m.buckets) <= idx {
		m.buckets = append(m.buckets, 0)
	}
	m.buckets[idx] += n
	m.mu.Unlock()
}

// peakMbps 从 from 起的 1 秒滑窗峰值
func (m *meter) peakMbps(from time.Time) float64 {
	if m == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	startIdx := int(from.Sub(m.start) / m.span)
	if startIdx < 0 {
		startIdx = 0
	}
	const w = 10 // 10 × 100ms = 1s
	var best int64
	for i := startIdx; i < len(m.buckets); i++ {
		var sum int64
		lo := i - w + 1
		if lo < startIdx {
			lo = startIdx
		}
		for j := lo; j <= i; j++ {
			sum += m.buckets[j]
		}
		if sum > best {
			best = sum
		}
	}
	return float64(best) * 8 / 1e6
}

// counter 累计字节数（作为 io.Writer 接入下载的 io.Copy）。
// 字节在真实到达时计入 measured（预热期只计入 total），
// 避免“整块计数在请求完成时刻”让平均值超过真实 1s 峰值。
type counter struct {
	total        atomic.Int64
	measured     atomic.Int64
	lastByte     atomic.Int64 // 最后一次计入 measured 的时间（unixNano）
	warmDeadline time.Time
	m            *meter
}

func (c *counter) Write(p []byte) (int, error) {
	n := int64(len(p))
	c.total.Add(n)
	c.m.add(n)
	if c.warmDeadline.IsZero() || time.Now().After(c.warmDeadline) {
		c.measured.Add(n)
		c.lastByte.Store(time.Now().UnixNano())
	}
	return len(p), nil
}

// finalize 统计平均速率与滑窗峰值；计量窗口封顶 maxSec，防止慢尾巴拉长窗口
func finalize(m *meter, measured int64, warmDeadline, lastByte time.Time, maxSec float64, workers, errs int) *PhaseResult {
	secs := lastByte.Sub(warmDeadline).Seconds()
	if secs < 1 {
		secs = 1
	}
	if secs > maxSec {
		secs = maxSec
	}
	return &PhaseResult{
		Mbps:     float64(measured) * 8 / secs / 1e6,
		PeakMbps: m.peakMbps(warmDeadline),
		MB:       float64(measured) / 1048576,
		Seconds:  secs,
		Workers:  workers,
		Errors:   errs,
	}
}

// phaseCtx 到 stop+grace 强制取消所有在途请求，保证阶段能准时收尾；
// 父 ctx 取消（关闭面板）时立即中止。
func phaseCtx(parent context.Context, stop time.Time) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	d := time.Until(stop.Add(phaseGrace))
	if d < 0 {
		d = 100 * time.Millisecond
	}
	timer := time.AfterFunc(d, cancel)
	return ctx, func() {
		timer.Stop()
		cancel()
	}
}

// reporter 实时进度，直到 workers 全部结束（或 ctx 截止后宽限 3s）
func reporter(ctx context.Context, wg *sync.WaitGroup, c *counter, progress func(mbps float64)) {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	lastT, lastB := time.Now(), c.total.Load()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			// 给在途请求一点收尾时间
			select {
			case <-done:
			case <-time.After(3 * time.Second):
			}
			return
		case t := <-tick.C:
			if progress != nil {
				dt := t.Sub(lastT).Seconds()
				progress(float64(c.total.Load()-lastB) * 8 / dt / 1e6)
				lastT = t
				lastB = c.total.Load()
			}
		}
	}
}

// phaseError 把停滞/取消/无有效数据翻译成可读错误；nil 表示阶段有效
func phaseError(parent context.Context, stalled bool, lastByte int64, errs int64, action string) error {
	if parent.Err() != nil {
		return errCanceled
	}
	if stalled {
		return fmt.Errorf("节点在 %s 内没有响应（连接被拒绝或重置）", stallTimeout)
	}
	if lastByte == 0 {
		return fmt.Errorf("%s阶段没有完成任何有效请求（%d 次错误）", action, errs)
	}
	return nil
}

func newRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Cache-Control", "no-cache")
	return req, nil
}

// withCacheBust 给 URL 加随机参数，绕开中间缓存
func withCacheBust(u string) string {
	sep := "?"
	if strings.Contains(u, "?") {
		sep = "&"
	}
	return u + sep + "x=" + strconv.Itoa(rand.Int())
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// noRedirect 上传不跟随重定向：301/302 会把 POST 改写成 GET（流量没上传却拿到 2xx），
// 307 的目标又常常不可达。统一把 3xx 当作失败。
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func newClient(workers int) *http.Client {
	tr := &http.Transport{
		MaxIdleConnsPerHost: workers,
		MaxConnsPerHost:     workers * 2,
		DisableCompression:  true, // 计量真实字节，禁止透明压缩
		IdleConnTimeout:     30 * time.Second,
	}
	return &http.Client{Transport: tr}
}

func newUploadClient(workers int) *http.Client {
	cli := newClient(workers)
	cli.CheckRedirect = noRedirect
	return cli
}

// runDownloadPhase speedtest.net 协议并行下载 random4000x4000.jpg
func runDownloadPhase(ctx context.Context, base string, workers int, warmup, measure time.Duration, progress func(mbps float64)) (*PhaseResult, error) {
	url := base + downloadFile
	var errs atomic.Int64

	start := time.Now()
	var c counter
	c.m = newMeter(start)
	warmDeadline := start.Add(warmup)
	c.warmDeadline = warmDeadline
	stop := start.Add(warmup + measure)
	phaseCtx, cancel := phaseCtx(ctx, stop)
	defer cancel()
	cli := newClient(workers)

	stalled := atomic.Bool{}
	watchdog := time.AfterFunc(stallTimeout, func() {
		if c.total.Load() == 0 {
			stalled.Store(true)
			cancel()
		}
	})
	defer watchdog.Stop()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) && phaseCtx.Err() == nil {
				req, err := newRequest(phaseCtx, http.MethodGet, withCacheBust(url), nil)
				if err != nil {
					return
				}
				resp, err := cli.Do(req)
				if err != nil {
					if phaseCtx.Err() == nil {
						errs.Add(1)
						sleepCtx(phaseCtx, retryDelay)
					}
					continue
				}
				if !isOK(resp.StatusCode) {
					// 限流页/错误页不计入流量，避免把错误页当成下载速度
					io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
					resp.Body.Close()
					errs.Add(1)
					continue
				}
				io.Copy(&c, resp.Body)
				resp.Body.Close()
			}
		}()
	}

	reporter(phaseCtx, &wg, &c, progress)
	if err := phaseError(ctx, stalled.Load(), c.lastByte.Load(), errs.Load(), "下载"); err != nil {
		return nil, err
	}
	res := finalize(c.m, c.measured.Load(), warmDeadline,
		time.Unix(0, c.lastByte.Load()), measure.Seconds()+phaseGrace.Seconds(), workers, int(errs.Load()))
	res.Source = "speedtest"
	return res, nil
}

func isOK(status int) bool { return status >= 200 && status < 300 }

// contentLength 取 CDN 大文件长度
func contentLength(url string) (int64, error) {
	cli := &http.Client{Timeout: 10 * time.Second}
	resp, err := cli.Head(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.ContentLength > 0 {
		return resp.ContentLength, nil
	}
	return 0, fmt.Errorf("无法获取文件大小")
}

// runCDNDownloadPhase 对 CDN 大文件做多连接 Range 下载（绕开 speedtest 节点限速）
func runCDNDownloadPhase(ctx context.Context, workers int, warmup, measure time.Duration, progress func(mbps float64)) (*PhaseResult, error) {
	size, err := contentLength(cdnFileURL)
	if err != nil {
		return nil, err
	}
	var errs atomic.Int64

	start := time.Now()
	var c counter
	c.m = newMeter(start)
	warmDeadline := start.Add(warmup)
	c.warmDeadline = warmDeadline
	stop := start.Add(warmup + measure)
	phaseCtx, cancel := phaseCtx(ctx, stop)
	defer cancel()
	cli := newClient(workers)

	stalled := atomic.Bool{}
	watchdog := time.AfterFunc(stallTimeout, func() {
		if c.total.Load() == 0 {
			stalled.Store(true)
			cancel()
		}
	})
	defer watchdog.Stop()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// 文件可能小于 workers×chunk：取模避免起始偏移越过 EOF 后一直 416
			off := (int64(idx) * cdnChunk) % size
			for time.Now().Before(stop) && phaseCtx.Err() == nil {
				end := off + cdnChunk - 1
				if end >= size {
					end = size - 1
				}
				req, err := newRequest(phaseCtx, http.MethodGet, cdnFileURL, nil)
				if err != nil {
					return
				}
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, end))
				resp, err := cli.Do(req)
				if err != nil {
					if phaseCtx.Err() == nil {
						errs.Add(1)
						sleepCtx(phaseCtx, retryDelay)
					}
					continue
				}
				if !isOK(resp.StatusCode) {
					io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
					resp.Body.Close()
					errs.Add(1)
					continue
				}
				io.Copy(&c, resp.Body)
				resp.Body.Close()
				off = (off + cdnChunk) % size
			}
		}(w)
	}

	reporter(phaseCtx, &wg, &c, progress)
	if err := phaseError(ctx, stalled.Load(), c.lastByte.Load(), errs.Load(), "CDN 下载"); err != nil {
		return nil, err
	}
	res := finalize(c.m, c.measured.Load(), warmDeadline,
		time.Unix(0, c.lastByte.Load()), measure.Seconds()+phaseGrace.Seconds(), workers, int(errs.Load()))
	res.Source = "CDN"
	return res, nil
}

// uploadRejected 节点以 HTTP 状态码拒绝了上传（常见于 413 请求体过大）
type uploadRejected struct{ status int }

func (e *uploadRejected) Error() string {
	return fmt.Sprintf("节点拒绝上传（HTTP %d）", e.status)
}

// buildUploadBody 生成 speedtest.net 协议要求的 content1=<随机数据> 表单体。
// 只用字母数字：随机二进制容易撞上 & = % 等分隔符，破坏表单编码。
func buildUploadBody(size int) []byte {
	raw := make([]byte, size)
	rand.Read(raw)
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	for i := range raw {
		raw[i] = alphabet[int(raw[i])%len(alphabet)]
	}
	return append([]byte("content1="), raw...)
}

// uploadReader 边发送边计量：请求体内的字节一离开进程就计入进度，
// 这样上传阶段也有实时速率（旧实现只在请求结束时计数，进度恒为 0）。
type uploadReader struct {
	r            io.Reader
	c            *counter
	warmDeadline time.Time
}

func (u *uploadReader) Read(p []byte) (int, error) {
	n, err := u.r.Read(p)
	if n > 0 {
		u.c.total.Add(int64(n))
		u.c.m.add(int64(n))
		if time.Now().After(u.warmDeadline) {
			u.c.measured.Add(int64(n))
			u.c.lastByte.Store(time.Now().UnixNano())
		}
	}
	return n, err
}

// probeUpload 用小块数据验证节点是否接受 POST 上传
func probeUpload(ctx context.Context, uploadURL string, size int) error {
	body := buildUploadBody(size)
	cli := &http.Client{Timeout: uploadProbeTimeout, CheckRedirect: noRedirect}
	req, err := newRequest(ctx, http.MethodPost, withCacheBust(uploadURL), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.ContentLength = int64(len(body))
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	if !isOK(resp.StatusCode) {
		return &uploadRejected{status: resp.StatusCode}
	}
	return nil
}

// uploadCandidates 从延迟排序的节点里筛出真正接受上传的节点（保持延迟顺序），
// 只探测前 maxUploadProbes 个，避免对 speedtest 网络造成无谓流量。
func uploadCandidates(ctx context.Context, ranked []serverProbe) []SpeedServer {
	probed := make([]serverProbe, 0, maxUploadProbes)
	seen := make(map[string]bool, maxUploadProbes)
	for _, r := range ranked {
		if len(probed) >= maxUploadProbes {
			break
		}
		if seen[r.server.Host] {
			continue
		}
		seen[r.server.Host] = true
		probed = append(probed, r)
	}

	ok := make([]bool, len(probed))
	var wg sync.WaitGroup
	for i, r := range probed {
		wg.Add(1)
		go func(i int, u string) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, uploadProbeTimeout)
			defer cancel()
			ok[i] = probeUpload(pctx, u, uploadProbeBytes) == nil
		}(i, r.server.URL)
	}
	wg.Wait()

	out := make([]SpeedServer, 0, len(probed))
	for i, r := range probed {
		if ok[i] {
			out = append(out, r.server)
		}
	}
	return out
}

// chunkChoices 返回降级序列（只会变小）：8→2→1；至少包含 1MB
func chunkChoices(chunkMB int) []int {
	if chunkMB <= 0 {
		chunkMB = 1
	}
	out := []int{chunkMB}
	for _, mb := range []int{2, 1} {
		if mb < chunkMB {
			out = append(out, mb)
		}
	}
	return out
}

// runUploadPhase 并行 POST 随机数据到 upload.php；节点明确拒绝大请求体时降级重试
func runUploadPhase(ctx context.Context, uploadURL string, workers int, warmup, measure time.Duration, chunkMB int, progress func(mbps float64)) (*PhaseResult, error) {
	var lastErr error
	for _, chunk := range chunkChoices(chunkMB) {
		if ctx.Err() != nil {
			return nil, errCanceled
		}
		res, err := uploadOnce(ctx, uploadURL, workers, warmup, measure, chunk, progress)
		if err == nil {
			return res, nil
		}
		lastErr = err
		var rej *uploadRejected
		if !errors.As(err, &rej) {
			return nil, err // 网络类失败（重置/超时）：换节点才有意义
		}
	}
	return nil, lastErr
}

func uploadOnce(ctx context.Context, uploadURL string, workers int, warmup, measure time.Duration, chunkMB int, progress func(mbps float64)) (*PhaseResult, error) {
	// speedtest.net 协议: form 表单字段 content1=<随机数据>
	form := buildUploadBody(chunkMB << 20)

	start := time.Now()
	var c counter
	c.m = newMeter(start)
	warmDeadline := start.Add(warmup)
	stop := start.Add(warmup + measure)
	phaseCtx, cancel := phaseCtx(ctx, stop)
	defer cancel()
	cli := newUploadClient(workers)

	var errs, rejected, successes atomic.Int64
	lastStatus := atomic.Int64{}
	stalled := atomic.Bool{}
	fastReject := atomic.Bool{}

	watchdog := time.AfterFunc(stallTimeout, func() {
		if c.total.Load() == 0 {
			stalled.Store(true)
			cancel()
		}
	})
	defer watchdog.Stop()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) && phaseCtx.Err() == nil {
				body := &uploadReader{
					r:            bytes.NewReader(form),
					c:            &c,
					warmDeadline: warmDeadline,
				}
				req, err := newRequest(phaseCtx, http.MethodPost, withCacheBust(uploadURL), body)
				if err != nil {
					return
				}
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				req.ContentLength = int64(len(form))
				resp, err := cli.Do(req)
				if err != nil {
					if phaseCtx.Err() == nil {
						errs.Add(1)
						sleepCtx(phaseCtx, retryDelay)
					}
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if !isOK(resp.StatusCode) {
					errs.Add(1)
					lastStatus.Store(int64(resp.StatusCode))
					rejected.Add(1)
					// 连续快速被拒且一次都没成功：这个节点不吃上传，别耗满整个计量窗
					if rejected.Load() >= 5 && successes.Load() == 0 {
						fastReject.Store(true)
						cancel()
					}
					continue
				}
				successes.Add(1)
			}
		}()
	}

	reporter(phaseCtx, &wg, &c, progress)
	if fastReject.Load() {
		return nil, &uploadRejected{status: int(lastStatus.Load())}
	}
	if err := phaseError(ctx, stalled.Load(), c.lastByte.Load(), errs.Load(), "上传"); err != nil {
		return nil, err
	}
	res := finalize(c.m, c.measured.Load(), warmDeadline,
		time.Unix(0, c.lastByte.Load()), measure.Seconds()+phaseGrace.Seconds(), workers, int(errs.Load()))
	res.Source = "speedtest"
	return res, nil
}
