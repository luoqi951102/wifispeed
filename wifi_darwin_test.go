//go:build darwin

package main

import "testing"

func TestParseWiFiLinkEnglish(t *testing.T) {
	out := `
    Interfaces:
      en0:
          Current Network Information:
            MyHomeWiFi:
              PHY Mode: 802.11ax
              Channel: 48 (5GHz, 80MHz)
              Security: WPA2 Personal
              Signal / Noise: -57 dBm / -91 dBm
            Other Local Wi-Fi Networks:
              Neighbour:
`
	w, err := parseWiFiLink(out)
	if err != nil {
		t.Fatalf("parseWiFiLink: %v", err)
	}
	if w.SSID != "MyHomeWiFi" || w.Redacted {
		t.Fatalf("ssid = %q redacted=%v", w.SSID, w.Redacted)
	}
	if w.PHY != "802.11ax" {
		t.Fatalf("phy = %q", w.PHY)
	}
	if w.Band != "5GHz" || w.Width != "80MHz" {
		t.Fatalf("band = %q width = %q", w.Band, w.Width)
	}
	if w.Signal != -57 || w.Noise != -91 || w.SNR != 34 {
		t.Fatalf("signal/noise/snr = %d/%d/%d", w.Signal, w.Noise, w.SNR)
	}
	if w.Quality() != "良好" {
		t.Fatalf("quality = %q, want 良好", w.Quality())
	}
}

func TestParseWiFiLinkRedactedChinese(t *testing.T) {
	out := `
      当前网络信息:
        <redacted>:
          PHY 模式: 802.11ax
          信道: 149 (5GHz, 80MHz)
          安全性: WPA2 个人级
          信号/噪声: -70 dBm / -92 dBm
        其他本地 Wi-Fi 网络:
`
	w, err := parseWiFiLink(out)
	if err != nil {
		t.Fatalf("parseWiFiLink: %v", err)
	}
	if !w.Redacted {
		t.Fatal("expected ssid_redacted for <redacted>")
	}
	if w.DisplaySSID() == redactedSSID {
		t.Fatal("DisplaySSID should explain redaction, not echo <redacted>")
	}
	if w.PHY != "802.11ax" || w.Band != "5GHz" {
		t.Fatalf("phy = %q band = %q", w.PHY, w.Band)
	}
	if w.SNR != 22 {
		t.Fatalf("snr = %d, want 22", w.SNR)
	}
}

func TestParseWiFiLinkNoNetwork(t *testing.T) {
	out := "    Interfaces:\n      en0:\n          Type: Ethernet\n"
	if _, err := parseWiFiLink(out); err == nil {
		t.Fatal("expected error when no current network section exists")
	}
}
