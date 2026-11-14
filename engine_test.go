package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMeterPeakUsesSlidingSecond(t *testing.T) {
	start := time.Now().Add(-5 * time.Second)
	m := &meter{start: start, span: 100 * time.Millisecond}
	m.buckets = make([]int64, 50)
	// 两段各 0.6s 的突发，中间空 0.4s：最大 1s 窗口只能取到单段
	for i := 10; i < 16; i++ {
		m.buckets[i] = 1_000_000
	}
	for i := 20; i < 26; i++ {
		m.buckets[i] = 1_000_000
	}

	// 6 × 1e6 bytes = 48 Mbit，落在 1s 窗口内 → 48 Mbps
	if got := m.peakMbps(start); math.Abs(got-48) > 1e-6 {
		t.Fatalf("peak = %.3f Mbps, want 48", got)
	}
}

func TestMeterNilSafe(t *testing.T) {
	var m *meter
	m.add(100)
	if got := m.peakMbps(time.Now()); got != 0 {
		t.Fatalf("nil meter peak = %v, want 0", got)
	}
}

func TestBuildLatencyStats(t *testing.T) {
	st := buildLatencyStats([]float64{10, 20, 30}, 1, 4)
	if st.MinMs != 10 || st.AvgMs != 20 || st.Samples != 3 {
		t.Fatalf("stats = %+v, want min 10 / avg 20 / samples 3", st)
	}
	if math.Abs(st.JitterMs-10) > 1e-9 {
		t.Fatalf("jitter = %v, want 10", st.JitterMs)
	}
	if math.Abs(st.LossPct-25) > 1e-9 {
		t.Fatalf("loss = %v, want 25", st.LossPct)
	}
}

func TestUploadReaderCountsPerByte(t *testing.T) {
	var c counter
	c.m = newMeter(time.Now())
	r := &uploadReader{r: strings.NewReader("0123456789"), c: &c, warmDeadline: time.Now().Add(-time.Millisecond)}
	if _, err := io.ReadAll(r); err != nil {
		t.Fatal(err)
	}
	if c.total.Load() != 10 || c.measured.Load() != 10 {
		t.Fatalf("total=%d measured=%d, want 10/10", c.total.Load(), c.measured.Load())
	}

	// 预热期内发送的字节只计入进度，不计入测量
	var c2 counter
	c2.m = newMeter(time.Now())
	r2 := &uploadReader{r: strings.NewReader("0123456789"), c: &c2, warmDeadline: time.Now().Add(time.Hour)}
	io.ReadAll(r2)
	if c2.total.Load() != 10 || c2.measured.Load() != 0 {
		t.Fatalf("total=%d measured=%d, want 10/0", c2.total.Load(), c2.measured.Load())
	}
}

func TestCounterWritesRespectWarmup(t *testing.T) {
	var c counter
	c.m = newMeter(time.Now())
	c.warmDeadline = time.Now().Add(-time.Millisecond)
	c.Write(make([]byte, 100))
	if c.total.Load() != 100 || c.measured.Load() != 100 || c.lastByte.Load() == 0 {
		t.Fatalf("total=%d measured=%d last=%d, want all counted", c.total.Load(), c.measured.Load(), c.lastByte.Load())
	}

	var warm counter
	warm.m = newMeter(time.Now())
	warm.warmDeadline = time.Now().Add(time.Hour)
	warm.Write(make([]byte, 100))
	if warm.total.Load() != 100 || warm.measured.Load() != 0 || warm.lastByte.Load() != 0 {
		t.Fatalf("total=%d measured=%d last=%d, want only total", warm.total.Load(), warm.measured.Load(), warm.lastByte.Load())
	}
}

func TestBuildUploadBody(t *testing.T) {
	body := buildUploadBody(1024)
	prefix := []byte("content1=")
	if !bytes.HasPrefix(body, prefix) {
		t.Fatal("missing content1= prefix")
	}
	if len(body) != 1024+len(prefix) {
		t.Fatalf("len = %d, want %d", len(body), 1024+len(prefix))
	}
	for _, ch := range body[len(prefix):] {
		isAlnum := ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9'
		if !isAlnum {
			t.Fatalf("body contains non-alnum byte %q (breaks form encoding)", ch)
		}
	}
}

func TestRunUploadPhaseMeasures(t *testing.T) {
	var received atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		received.Add(n)
		io.WriteString(w, "size="+strconv.FormatInt(n, 10))
	}))
	defer srv.Close()

	res, err := runUploadPhase(context.Background(), srv.URL+"/speedtest/upload.php",
		4, 200*time.Millisecond, 500*time.Millisecond, 1, nil)
	if err != nil {
		t.Fatalf("runUploadPhase: %v", err)
	}
	if res.Mbps <= 0 || res.MB <= 0 || res.PeakMbps <= 0 {
		t.Fatalf("result = %+v, want positive throughput", res)
	}
	if res.Errors != 0 {
		t.Fatalf("errors = %d, want 0", res.Errors)
	}
	if received.Load() == 0 {
		t.Fatal("server received no upload bytes")
	}
}

func TestRunUploadPhaseRejectsRedirect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/login", http.StatusFound)
	}))
	defer srv.Close()

	// 301/302 会把 POST 改写成 GET：必须当失败，不能把重定向页算成上传成功
	_, err := runUploadPhase(context.Background(), srv.URL+"/speedtest/upload.php",
		2, 100*time.Millisecond, 300*time.Millisecond, 1, nil)
	if err == nil {
		t.Fatal("expected redirect to be rejected, got nil error")
	}
}

func TestRunUploadPhaseRejectsNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := runUploadPhase(context.Background(), srv.URL+"/speedtest/upload.php",
		2, 100*time.Millisecond, 300*time.Millisecond, 1, nil)
	if err == nil {
		t.Fatal("expected 403 to be rejected, got nil error")
	}
}

func TestProbeUploadRejectsPayloadTooLarge(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body) // 先读完再回，保证客户端能稳定收到响应
		if r.ContentLength > 512<<10 {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	err := probeUpload(context.Background(), srv.URL+"/upload.php", 1<<20)
	var rej *uploadRejected
	if !errors.As(err, &rej) || rej.status != http.StatusRequestEntityTooLarge {
		t.Fatalf("err = %v, want uploadRejected 413", err)
	}
}

func TestUploadCandidatesSkipsRejectingNodes(t *testing.T) {
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer good.Close()

	ranked := []serverProbe{
		{server: SpeedServer{URL: bad.URL + "/speedtest/upload.php", Host: "bad"}, stats: &LatencyStats{AvgMs: 1}},
		{server: SpeedServer{URL: good.URL + "/speedtest/upload.php", Host: "good"}, stats: &LatencyStats{AvgMs: 2}},
	}
	got := uploadCandidates(context.Background(), ranked)
	if len(got) != 1 || got[0].Host != "good" {
		t.Fatalf("got %+v, want only the good node", got)
	}
}

func TestRunDownloadPhaseMeasures(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 256<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(payload)
	}))
	defer srv.Close()

	res, err := runDownloadPhase(context.Background(), srv.URL+"/",
		4, 200*time.Millisecond, 500*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("runDownloadPhase: %v", err)
	}
	if res.Mbps <= 0 || res.MB <= 0 {
		t.Fatalf("result = %+v, want positive throughput", res)
	}
}

func TestRunDownloadPhaseIgnoresErrorPages(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, strings.Repeat("denied", 1000))
	}))
	defer srv.Close()

	_, err := runDownloadPhase(context.Background(), srv.URL+"/",
		2, 100*time.Millisecond, 300*time.Millisecond, nil)
	if err == nil {
		t.Fatal("403 page must not count as downloaded data")
	}
}

func TestRunDownloadPhaseCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(bytes.Repeat([]byte("x"), 64<<10))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	_, err := runDownloadPhase(ctx, srv.URL+"/", 2, 50*time.Millisecond, 5*time.Second, nil)
	if !errors.Is(err, errCanceled) {
		t.Fatalf("err = %v, want errCanceled", err)
	}
}

func TestChunkChoices(t *testing.T) {
	cases := []struct {
		in   int
		want []int
	}{
		{8, []int{8, 2, 1}},
		{2, []int{2, 1}},
		{1, []int{1}},
		{0, []int{1}},
	}
	for _, c := range cases {
		got := chunkChoices(c.in)
		if len(got) != len(c.want) {
			t.Fatalf("chunkChoices(%d) = %v, want %v", c.in, got, c.want)
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Fatalf("chunkChoices(%d) = %v, want %v", c.in, got, c.want)
			}
		}
	}
}

func TestMeasureLatency(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "test=test")
	}))
	defer srv.Close()

	st, err := measureLatency(context.Background(), srv.URL+"/", 4)
	if err != nil {
		t.Fatalf("measureLatency: %v", err)
	}
	if st.Samples != 4 || st.LossPct != 0 || st.AvgMs <= 0 || st.MinMs <= 0 {
		t.Fatalf("stats = %+v, want 4 successful samples", st)
	}
}

func TestProbeServersOrdersByLatency(t *testing.T) {
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "ok")
	}))
	defer fast.Close()
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(80 * time.Millisecond)
		io.WriteString(w, "ok")
	}))
	defer slow.Close()

	ranked := probeServers(context.Background(), []SpeedServer{
		{URL: slow.URL + "/speedtest/upload.php", Host: "slow"},
		{URL: fast.URL + "/speedtest/upload.php", Host: "fast"},
	})
	if len(ranked) != 2 || ranked[0].server.Host != "fast" {
		t.Fatalf("ranked = %+v, want fast node first", ranked)
	}
}

func TestPhaseError(t *testing.T) {
	if err := phaseError(context.Background(), false, 100, 0, "下载"); err != nil {
		t.Fatalf("healthy phase returned %v", err)
	}
	if err := phaseError(context.Background(), true, 100, 0, "下载"); err == nil {
		t.Fatal("stalled phase should return an error")
	}
	err := phaseError(context.Background(), false, 0, 7, "上传")
	if err == nil || !strings.Contains(err.Error(), "7 次错误") {
		t.Fatalf("err = %v, want error mentioning 7 errors", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(phaseError(ctx, false, 100, 0, "下载"), errCanceled) {
		t.Fatal("canceled parent should map to errCanceled")
	}
}

func TestRunUploadPhaseDowngradesOn413(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if r.ContentLength > 1<<20+64 { // 1MB 块带 content1= 前缀，阈值留出余量
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	res, err := runUploadPhase(context.Background(), srv.URL+"/speedtest/upload.php",
		2, 100*time.Millisecond, 400*time.Millisecond, 2, nil)
	if err != nil {
		t.Fatalf("runUploadPhase should downgrade 2MB→1MB on 413, got %v", err)
	}
	if res.MB <= 0 {
		t.Fatalf("result = %+v, want positive MB", res)
	}
}

func TestRunCDNDownloadPhase(t *testing.T) {
	payload := bytes.Repeat([]byte("z"), 300<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			return
		}
		start, end, ok := parseByteRange(r.Header.Get("Range"), int64(len(payload)))
		if !ok {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(payload[start : end+1])
	}))
	defer srv.Close()

	old := cdnFileURL
	cdnFileURL = srv.URL + "/file.bin"
	defer func() { cdnFileURL = old }()

	res, err := runCDNDownloadPhase(context.Background(), 4, 100*time.Millisecond, 400*time.Millisecond, nil)
	if err != nil {
		t.Fatalf("runCDNDownloadPhase: %v", err)
	}
	if res.MB <= 0 || res.Source != "CDN" {
		t.Fatalf("result = %+v, want positive CDN result", res)
	}
}

func parseByteRange(h string, size int64) (int64, int64, bool) {
	if !strings.HasPrefix(h, "bytes=") {
		return 0, 0, false
	}
	parts := strings.SplitN(strings.TrimPrefix(h, "bytes="), "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	start, err1 := strconv.ParseInt(parts[0], 10, 64)
	end, err2 := strconv.ParseInt(parts[1], 10, 64)
	if err1 != nil || err2 != nil || start < 0 || end < start || end >= size {
		return 0, 0, false
	}
	return start, end, true
}

// TestRunTestAgainstLocalServer 用本地假节点跑完整流程：延迟 → 下载 → 上传 → 结论
func TestRunTestAgainstLocalServer(t *testing.T) {
	payload := bytes.Repeat([]byte("p"), 64<<10)
	mux := http.NewServeMux()
	mux.HandleFunc("/speedtest/latency.txt", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "test=test")
	})
	mux.HandleFunc("/speedtest/random4000x4000.jpg", func(w http.ResponseWriter, r *http.Request) {
		w.Write(payload)
	})
	mux.HandleFunc("/speedtest/upload.php", func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		io.WriteString(w, "size="+strconv.FormatInt(n, 10))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := config{
		dlConc: 2, upConc: 2, warmSec: 0, measureSec: 1, chunkMB: 1,
		serverURL: srv.URL + "/speedtest/", noCDN: true,
	}.normalized()

	var mu sync.Mutex
	seen := map[string]bool{}
	report := runTest(context.Background(), cfg, func(ev event) {
		mu.Lock()
		seen[ev["type"].(string)] = true
		mu.Unlock()
	})

	if report.Server == nil || report.Latency == nil {
		t.Fatalf("report missing server/latency: %+v", report)
	}
	if report.Download == nil || report.Download.Mbps <= 0 {
		t.Fatalf("report download = %+v, want positive", report.Download)
	}
	if report.Upload == nil || report.Upload.Mbps <= 0 {
		t.Fatalf("report upload = %+v, want positive", report.Upload)
	}
	if report.UploadServer == nil || report.UploadServer.Host != report.Server.Host {
		t.Fatalf("upload server = %+v, want manual server", report.UploadServer)
	}
	for _, want := range []string{"banner", "server", "latency", "phase", "result", "verdict", "done"} {
		if !seen[want] {
			t.Fatalf("missing event %q (got %v)", want, seen)
		}
	}
}
