//go:build darwin

package main

// macOS Wi-Fi 链路诊断：解析 system_profiler SPAirPortDataType 的当前网络段。
// system_profiler 的输出跟随系统语言（英文 / 中文键名），两种都支持。

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// redactedSSID macOS 13+ 在未授予定位权限时会隐藏 SSID
const redactedSSID = "<redacted>"

// WiFiLink 当前 Wi-Fi 链路信息
type WiFiLink struct {
	SSID     string `json:"ssid"`
	Redacted bool   `json:"ssid_redacted"`
	PHY      string `json:"phy"`
	Channel  string `json:"channel"`
	Band     string `json:"band"`
	Width    string `json:"width"`
	Security string `json:"security"`
	Signal   int    `json:"signal_dbm"`
	Noise    int    `json:"noise_dbm"`
	SNR      int    `json:"snr_db"`
}

// DisplaySSID 隐藏 SSID 时给出可操作的解释，而不是显示 <redacted>
func (w *WiFiLink) DisplaySSID() string {
	if w.Redacted {
		return "已隐藏（macOS 隐私限制，需为终端授予定位权限）"
	}
	if w.SSID == "" {
		return "（未知）"
	}
	return w.SSID
}

// Quality 按 SNR 给链路质量评级
func (w *WiFiLink) Quality() string {
	switch {
	case w.SNR >= 35:
		return "优秀"
	case w.SNR >= 25:
		return "良好"
	case w.SNR >= 18:
		return "一般"
	default:
		return "较差"
	}
}

func readWiFiLink() (*WiFiLink, error) {
	out, err := exec.Command("system_profiler", "SPAirPortDataType").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("执行 system_profiler 失败: %w", err)
	}
	return parseWiFiLink(string(out))
}

func parseWiFiLink(out string) (*WiFiLink, error) {
	lines := strings.Split(out, "\n")
	idx := -1
	for i, l := range lines {
		if strings.Contains(l, "Current Network") || strings.Contains(l, "当前网络") {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, fmt.Errorf("未找到当前 Wi-Fi 网络（可能未连接 Wi-Fi 或使用有线）")
	}

	w := &WiFiLink{}
	for _, l := range lines[idx+1 : min(idx+40, len(lines))] {
		t := strings.TrimSpace(l)
		if t == "" || isNetworkSectionEnd(l) {
			break
		}
		// "网络名:" 单独一行的样式（SSID 行，可能被系统隐藏为 <redacted>）
		if strings.HasSuffix(t, ":") && !strings.Contains(t, ": ") && w.SSID == "" {
			w.SSID = strings.TrimSuffix(t, ":")
			w.Redacted = w.SSID == redactedSSID
			continue
		}
		k, v, ok := strings.Cut(t, ": ")
		if !ok {
			break // 离开当前网络小节
		}
		applyWiFiField(w, k, v)
	}
	if w.PHY == "" && w.SSID == "" {
		return nil, fmt.Errorf("解析 Wi-Fi 信息失败")
	}
	return w, nil
}

func isNetworkSectionEnd(l string) bool {
	for _, marker := range []string{
		"Other Local Wi-Fi Networks", "Other Networks",
		"其他本地", "其他网络",
	} {
		if strings.Contains(l, marker) {
			return true
		}
	}
	return false
}

func applyWiFiField(w *WiFiLink, k, v string) {
	switch k {
	case "PHY Mode", "PHY 模式":
		w.PHY = v
	case "Channel", "信道":
		w.Channel = v
		if f := strings.Fields(v); len(f) >= 2 {
			w.Band = strings.Trim(f[1], "(,")
		}
		for _, f := range strings.Fields(v) {
			if strings.HasSuffix(f, "MHz)") {
				w.Width = strings.TrimSuffix(f, ")")
			}
		}
	case "Security", "安全性":
		w.Security = v
	case "Signal / Noise", "信号/噪声", "信号 / 噪声":
		nums := make([]int, 0, 2)
		for _, part := range strings.Fields(v) {
			if n, err := strconv.Atoi(strings.TrimSuffix(part, "dBm")); err == nil {
				nums = append(nums, n)
			}
		}
		if len(nums) >= 2 {
			w.Signal, w.Noise = nums[0], nums[1]
			w.SNR = w.Signal - w.Noise
		}
	}
}
