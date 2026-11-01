package main

import (
	"net"
	"net/http"
	"sync"
)

// 30mb per day per ip
var quota = NewQuota(30 << 20)

type Quota struct {
	mu    sync.Mutex
	used  map[string]int64
	limit int64
}

func NewQuota(limit int64) *Quota {
	return &Quota{used: make(map[string]int64), limit: limit}
}

// Allow writes off cost bytes from the IP. false means the limit for today has been reached
func (q *Quota) Allow(ip string, cost int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.used[ip]+cost > q.limit {
		return false
	}
	q.used[ip] += cost
	return true
}

// Reset resets all counters, called once a day
func (q *Quota) Reset() {
	q.mu.Lock()
	q.used = make(map[string]int64)
	q.mu.Unlock()
}

// clientIP takes the address from the nginx header. IPv6 rolls down to 64
func clientIP(r *http.Request) string {
	raw := r.Header.Get("X-Real-IP")
	if raw == "" {
		raw, _, _ = net.SplitHostPort(r.RemoteAddr)
	}
	ip := net.ParseIP(raw)
	if ip == nil {
		return raw
	}
	if ip.To4() == nil {
		ip = ip.Mask(net.CIDRMask(64, 128))
	}
	return ip.String()
}