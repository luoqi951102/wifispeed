package main

// Web 可视化面板：内嵌静态页 + NDJSON 事件流 API
// （可视化形态对齐 librespeed/speedtest ★15.3k 的 HTML5 仪表盘）

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

//go:embed static
var staticFS embed.FS

var webBusy atomic.Bool

// serveWeb 启动面板服务（阻塞）
func serveWeb(addr string, cfg config) error {
	mux := http.NewServeMux()
	sub, _ := fs.Sub(staticFS, "static") // 把 static 子目录挂到根路径，避免 /static/ 前缀
	mux.Handle("/", http.FileServer(http.FS(sub)))

	mux.HandleFunc("/api/wifi", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		link, err := readWiFiLink()
		if err != nil {
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(link)
	})

	mux.HandleFunc("/api/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "只支持 POST", http.StatusMethodNotAllowed)
			return
		}
		if !sameOrigin(r) {
			http.Error(w, "拒绝跨站请求", http.StatusForbidden)
			return
		}
		if !webBusy.CompareAndSwap(false, true) {
			http.Error(w, "已有测试在进行中", http.StatusConflict)
			return
		}
		defer webBusy.Store(false)
		w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		flusher, canFlush := w.(http.Flusher)
		// 客户端断开（关闭/刷新页面）时 r.Context() 取消，测试随之中止，
		// 不会让 webBusy 一直占用导致下次点击报 409。
		runTest(r.Context(), cfg, func(ev event) {
			b, err := json.Marshal(ev)
			if err != nil {
				return
			}
			fmt.Fprintf(w, "%s\n", b)
			if canFlush {
				flusher.Flush()
			}
		})
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return srv.ListenAndServe()
}

// sameOrigin 阻止别的网页跨站触发本机测速（CSRF）。
// 浏览器跨站 POST 会带 Origin 或 Sec-Fetch-Site: cross-site；
// curl 等本地客户端不带 Origin，放行。
func sameOrigin(r *http.Request) bool {
	if strings.EqualFold(r.Header.Get("Sec-Fetch-Site"), "cross-site") {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && u.Host == r.Host
}

// openBrowser 在默认浏览器打开面板
func openBrowser(url string) {
	go func() {
		time.Sleep(400 * time.Millisecond) // 等端口就绪
		var cmd *exec.Cmd
		switch runtime.GOOS {
		case "darwin":
			cmd = exec.Command("open", url)
		case "windows":
			cmd = exec.Command("cmd", "/c", "start", url)
		default:
			cmd = exec.Command("xdg-open", url)
		}
		cmd.Start()
	}()
}
