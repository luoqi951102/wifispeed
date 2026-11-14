package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSameOrigin(t *testing.T) {
	cases := []struct {
		name, origin, site, host string
		want                     bool
	}{
		{"无 Origin（curl / 本地客户端）", "", "", "127.0.0.1:8187", true},
		{"同源 POST", "http://127.0.0.1:8187", "same-origin", "127.0.0.1:8187", true},
		{"localhost 同源", "http://localhost:8187", "same-origin", "localhost:8187", true},
		{"跨站 Origin", "http://evil.example", "cross-site", "127.0.0.1:8187", false},
		{"只有 Sec-Fetch-Site", "", "cross-site", "127.0.0.1:8187", false},
		{"Origin 无法解析", "::bad::", "", "127.0.0.1:8187", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, "http://"+c.host+"/api/run", nil)
		if c.origin != "" {
			r.Header.Set("Origin", c.origin)
		}
		if c.site != "" {
			r.Header.Set("Sec-Fetch-Site", c.site)
		}
		if got := sameOrigin(r); got != c.want {
			t.Fatalf("%s: sameOrigin = %v, want %v", c.name, got, c.want)
		}
	}
}
