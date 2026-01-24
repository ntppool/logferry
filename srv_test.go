package main

import (
	"context"
	"errors"
	"net"
	"net/url"
	"testing"
	"time"
)

// TestDeriveSRVDomain tests the SRV domain derivation from hostnames.
func TestDeriveSRVDomain(t *testing.T) {
	tests := []struct {
		hostname string
		want     string
	}{
		{"logferry-api.ntppool.net", "ntppool.net"},
		{"api.logs.example.com", "logs.example.com"},
		{"server.deep.nested.domain.org", "deep.nested.domain.org"},
		{"example.com", "example.com"}, // Two-part domain stays as-is
		{"localhost", "localhost"},     // Single label stays as-is
		{"a.b.c.d.e.f", "b.c.d.e.f"},   // Deep nesting
	}

	for _, tt := range tests {
		t.Run(tt.hostname, func(t *testing.T) {
			got := deriveSRVDomain(tt.hostname)
			if got != tt.want {
				t.Errorf("deriveSRVDomain(%q) = %q, want %q", tt.hostname, got, tt.want)
			}
		})
	}
}

// TestSRVTargetAddress tests the Address() method.
func TestSRVTargetAddress(t *testing.T) {
	tests := []struct {
		host string
		port uint16
		want string
	}{
		{"server1.example.com", 443, "server1.example.com:443"},
		{"server2.example.com", 8443, "server2.example.com:8443"},
		{"192.0.2.1", 443, "192.0.2.1:443"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			target := SRVTarget{Host: tt.host, Port: tt.port}
			if got := target.Address(); got != tt.want {
				t.Errorf("Address() = %q, want %q", got, tt.want)
			}
		})
	}
}

// mockResolver creates a test resolver function.
func mockResolver(targets []*net.SRV, err error) func(context.Context, string, string, string) (string, []*net.SRV, error) {
	return func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
		return "", targets, err
	}
}

// TestTargetPoolResolveSRV tests SRV resolution with various scenarios.
func TestTargetPoolResolveSRV(t *testing.T) {
	upstreamURL, _ := url.Parse("https://logferry-api.example.com/")

	t.Run("successful resolution", func(t *testing.T) {
		tp := NewTargetPool(upstreamURL, nil)
		tp.resolver = mockResolver([]*net.SRV{
			{Target: "server1.example.com.", Port: 443, Priority: 10, Weight: 50},
			{Target: "server2.example.com.", Port: 443, Priority: 10, Weight: 50},
		}, nil)

		result := tp.ResolveSRV(context.Background())
		if !result {
			t.Error("ResolveSRV returned false, expected true")
		}
		if tp.IsFallbackMode() {
			t.Error("TargetPool should not be in fallback mode after successful resolution")
		}
		if tp.TargetCount() != 2 {
			t.Errorf("TargetCount() = %d, want 2", tp.TargetCount())
		}
	})

	t.Run("empty targets falls back", func(t *testing.T) {
		tp := NewTargetPool(upstreamURL, nil)
		tp.resolver = mockResolver([]*net.SRV{}, nil)

		result := tp.ResolveSRV(context.Background())
		if result {
			t.Error("ResolveSRV returned true, expected false")
		}
		if !tp.IsFallbackMode() {
			t.Error("TargetPool should be in fallback mode")
		}
	})

	t.Run("error falls back", func(t *testing.T) {
		tp := NewTargetPool(upstreamURL, nil)
		tp.resolver = mockResolver(nil, errors.New("DNS lookup failed"))

		result := tp.ResolveSRV(context.Background())
		if result {
			t.Error("ResolveSRV returned true, expected false")
		}
		if !tp.IsFallbackMode() {
			t.Error("TargetPool should be in fallback mode")
		}
	})

	t.Run("trailing dot removed from hostname", func(t *testing.T) {
		tp := NewTargetPool(upstreamURL, nil)
		tp.resolver = mockResolver([]*net.SRV{
			{Target: "server1.example.com.", Port: 443, Priority: 10, Weight: 50},
		}, nil)

		tp.ResolveSRV(context.Background())

		// Get the target URL and check the hostname
		targetURL := tp.SelectTarget()
		if targetURL != "https://server1.example.com:443/" {
			t.Errorf("SelectTarget() = %q, expected no trailing dot in hostname", targetURL)
		}
	})
}

// TestTargetPoolSelectTarget tests target selection logic.
func TestTargetPoolSelectTarget(t *testing.T) {
	upstreamURL, _ := url.Parse("https://logferry-api.example.com/")

	t.Run("fallback mode returns upstream URL", func(t *testing.T) {
		tp := NewTargetPool(upstreamURL, nil)

		targetURL := tp.SelectTarget()
		if targetURL != "https://logferry-api.example.com/" {
			t.Errorf("SelectTarget() = %q, want upstream URL", targetURL)
		}
	})

	t.Run("single target", func(t *testing.T) {
		tp := NewTargetPool(upstreamURL, nil)
		tp.resolver = mockResolver([]*net.SRV{
			{Target: "server1.example.com", Port: 443, Priority: 10, Weight: 100},
		}, nil)
		tp.ResolveSRV(context.Background())

		targetURL := tp.SelectTarget()
		if targetURL != "https://server1.example.com:443/" {
			t.Errorf("SelectTarget() = %q, want server1 URL", targetURL)
		}
	})

	t.Run("priority ordering", func(t *testing.T) {
		tp := NewTargetPool(upstreamURL, nil)
		tp.resolver = mockResolver([]*net.SRV{
			{Target: "backup.example.com", Port: 443, Priority: 20, Weight: 100},
			{Target: "primary.example.com", Port: 443, Priority: 10, Weight: 100},
		}, nil)
		tp.ResolveSRV(context.Background())

		// Mark primary as unhealthy
		tp.MarkUnhealthy("primary.example.com:443", errors.New("test"))

		// Should now select backup
		targetURL := tp.SelectTarget()
		if targetURL != "https://backup.example.com:443/" {
			t.Errorf("SelectTarget() = %q, expected backup server", targetURL)
		}
	})
}

// TestTargetPoolHealthTracking tests health state management.
func TestTargetPoolHealthTracking(t *testing.T) {
	upstreamURL, _ := url.Parse("https://logferry-api.example.com/")

	t.Run("mark healthy", func(t *testing.T) {
		tp := NewTargetPool(upstreamURL, nil)
		tp.resolver = mockResolver([]*net.SRV{
			{Target: "server1.example.com", Port: 443, Priority: 10, Weight: 100},
		}, nil)
		tp.ResolveSRV(context.Background())

		// Initially healthy
		tp.MarkHealthy("server1.example.com:443")

		// Should still select the target
		targetURL := tp.SelectTarget()
		if targetURL != "https://server1.example.com:443/" {
			t.Errorf("SelectTarget() = %q, expected server1", targetURL)
		}
	})

	t.Run("mark unhealthy triggers backoff", func(t *testing.T) {
		tp := NewTargetPool(upstreamURL, nil)
		tp.resolver = mockResolver([]*net.SRV{
			{Target: "server1.example.com", Port: 443, Priority: 10, Weight: 100},
			{Target: "server2.example.com", Port: 443, Priority: 10, Weight: 100},
		}, nil)
		tp.ResolveSRV(context.Background())

		// Mark server1 as unhealthy
		tp.MarkUnhealthy("server1.example.com:443", errors.New("connection failed"))

		// Check that server1 is in backoff
		tp.mu.RLock()
		info := tp.healthState["server1.example.com:443"]
		tp.mu.RUnlock()

		if info.healthy {
			t.Error("server1 should be unhealthy")
		}
		if info.backoffUntil.Before(time.Now()) {
			t.Error("backoff should be in the future")
		}
	})

	t.Run("recovery resets backoff", func(t *testing.T) {
		tp := NewTargetPool(upstreamURL, nil)
		tp.resolver = mockResolver([]*net.SRV{
			{Target: "server1.example.com", Port: 443, Priority: 10, Weight: 100},
		}, nil)
		tp.ResolveSRV(context.Background())

		// Mark unhealthy then healthy
		tp.MarkUnhealthy("server1.example.com:443", errors.New("test"))
		tp.MarkHealthy("server1.example.com:443")

		tp.mu.RLock()
		info := tp.healthState["server1.example.com:443"]
		tp.mu.RUnlock()

		if !info.healthy {
			t.Error("server1 should be healthy after recovery")
		}
		if info.backoffLevel != 0 {
			t.Errorf("backoff level should be reset, got %d", info.backoffLevel)
		}
	})
}

// TestBackoffDurations verifies the backoff schedule.
func TestBackoffDurations(t *testing.T) {
	expected := []time.Duration{
		5 * time.Second,
		10 * time.Second,
		20 * time.Second,
		40 * time.Second,
		80 * time.Second,
		160 * time.Second,
		300 * time.Second,
	}

	if len(backoffDurations) != len(expected) {
		t.Fatalf("backoffDurations length = %d, want %d", len(backoffDurations), len(expected))
	}

	for i, want := range expected {
		if backoffDurations[i] != want {
			t.Errorf("backoffDurations[%d] = %v, want %v", i, backoffDurations[i], want)
		}
	}
}

// TestSelectLeastUnhealthy tests selection when all targets are unhealthy.
func TestSelectLeastUnhealthy(t *testing.T) {
	upstreamURL, _ := url.Parse("https://logferry-api.example.com/")
	tp := NewTargetPool(upstreamURL, nil)
	tp.resolver = mockResolver([]*net.SRV{
		{Target: "server1.example.com", Port: 443, Priority: 10, Weight: 100},
		{Target: "server2.example.com", Port: 443, Priority: 10, Weight: 100},
	}, nil)
	tp.ResolveSRV(context.Background())

	// Mark both as unhealthy with different backoff levels
	tp.MarkUnhealthy("server1.example.com:443", errors.New("test"))
	time.Sleep(10 * time.Millisecond) // Small delay
	tp.MarkUnhealthy("server2.example.com:443", errors.New("test"))

	// Both are unhealthy, should select one (the one with shorter remaining backoff)
	targetURL := tp.SelectTarget()

	// Should return a valid target URL (not the upstream URL)
	if targetURL == "https://logferry-api.example.com/" {
		t.Error("Should select from unhealthy targets when all are unhealthy")
	}
}

// TestGetTargetAddress tests URL parsing for health tracking.
func TestGetTargetAddress(t *testing.T) {
	tests := []struct {
		url  string
		want string
	}{
		{"https://server1.example.com:443/", "server1.example.com:443"},
		{"https://server2.example.com:8443/api/v1", "server2.example.com:8443"},
		{"https://example.com/", "example.com"},
		{"invalid", ""},
	}

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			got := GetTargetAddress(tt.url)
			if got != tt.want {
				t.Errorf("GetTargetAddress(%q) = %q, want %q", tt.url, got, tt.want)
			}
		})
	}
}

// TestWeightDistribution tests that weight-based selection is statistically correct.
func TestWeightDistribution(t *testing.T) {
	upstreamURL, _ := url.Parse("https://logferry-api.example.com/")
	tp := NewTargetPool(upstreamURL, nil)
	tp.resolver = mockResolver([]*net.SRV{
		{Target: "heavy.example.com", Port: 443, Priority: 10, Weight: 90},
		{Target: "light.example.com", Port: 443, Priority: 10, Weight: 10},
	}, nil)
	tp.ResolveSRV(context.Background())

	// Run many selections and count distribution
	counts := make(map[string]int)
	iterations := 10000

	for i := 0; i < iterations; i++ {
		targetURL := tp.SelectTarget()
		counts[targetURL]++
	}

	heavyCount := counts["https://heavy.example.com:443/"]
	lightCount := counts["https://light.example.com:443/"]

	// With 90/10 weight ratio, heavy should get ~90% of selections
	// Allow 5% margin for randomness
	heavyRatio := float64(heavyCount) / float64(iterations)
	if heavyRatio < 0.85 || heavyRatio > 0.95 {
		t.Errorf("Weight distribution incorrect: heavy=%.2f%%, expected ~90%%", heavyRatio*100)
	}

	t.Logf("Distribution: heavy=%d (%.1f%%), light=%d (%.1f%%)",
		heavyCount, heavyRatio*100, lightCount, float64(lightCount)/float64(iterations)*100)
}

// TestZeroWeightTreatedAsOne tests RFC 2782 weight=0 handling.
func TestZeroWeightTreatedAsOne(t *testing.T) {
	upstreamURL, _ := url.Parse("https://logferry-api.example.com/")
	tp := NewTargetPool(upstreamURL, nil)
	tp.resolver = mockResolver([]*net.SRV{
		{Target: "server1.example.com", Port: 443, Priority: 10, Weight: 0},
		{Target: "server2.example.com", Port: 443, Priority: 10, Weight: 0},
	}, nil)
	tp.ResolveSRV(context.Background())

	// Both have weight 0 (treated as 1), should distribute roughly equally
	counts := make(map[string]int)
	iterations := 1000

	for i := 0; i < iterations; i++ {
		targetURL := tp.SelectTarget()
		counts[targetURL]++
	}

	server1Count := counts["https://server1.example.com:443/"]
	ratio := float64(server1Count) / float64(iterations)

	// Should be roughly 50/50 with weight=0 (treated as 1 each)
	if ratio < 0.40 || ratio > 0.60 {
		t.Errorf("Zero weight distribution incorrect: server1=%.1f%%, expected ~50%%", ratio*100)
	}
}

// TestStaleHealthStateCleanup tests that stale health state is cleaned up.
func TestStaleHealthStateCleanup(t *testing.T) {
	upstreamURL, _ := url.Parse("https://logferry-api.example.com/")
	tp := NewTargetPool(upstreamURL, nil)

	// Initial resolve with server1
	tp.resolver = mockResolver([]*net.SRV{
		{Target: "server1.example.com", Port: 443, Priority: 10, Weight: 100},
	}, nil)
	tp.ResolveSRV(context.Background())

	// Mark server1 unhealthy to create health state
	tp.MarkUnhealthy("server1.example.com:443", errors.New("test"))

	// Now resolve without server1 (simulate removal)
	tp.resolver = mockResolver([]*net.SRV{
		{Target: "server2.example.com", Port: 443, Priority: 10, Weight: 100},
	}, nil)

	// First refresh - server1 should still have health state (staleness = 1)
	tp.ResolveSRV(context.Background())
	tp.mu.RLock()
	_, hasServer1 := tp.healthState["server1.example.com:443"]
	tp.mu.RUnlock()
	if !hasServer1 {
		t.Error("server1 health state should still exist after 1 refresh")
	}

	// Second refresh - server1 should still have health state (staleness = 2)
	tp.ResolveSRV(context.Background())
	tp.mu.RLock()
	_, hasServer1 = tp.healthState["server1.example.com:443"]
	tp.mu.RUnlock()
	if !hasServer1 {
		t.Error("server1 health state should still exist after 2 refreshes")
	}

	// Third refresh - server1 health state should be cleaned up (staleness = 3)
	tp.ResolveSRV(context.Background())
	tp.mu.RLock()
	_, hasServer1 = tp.healthState["server1.example.com:443"]
	tp.mu.RUnlock()
	if hasServer1 {
		t.Error("server1 health state should be cleaned up after 3 refreshes")
	}
}

// TestNeedsRefresh tests TTL-based refresh checking.
func TestNeedsRefresh(t *testing.T) {
	upstreamURL, _ := url.Parse("https://logferry-api.example.com/")
	tp := NewTargetPool(upstreamURL, nil)

	// Before any lookup, should need refresh
	if !tp.NeedsRefresh(time.Minute) {
		t.Error("Should need refresh before first lookup")
	}

	// After lookup, should not need refresh immediately
	tp.resolver = mockResolver([]*net.SRV{
		{Target: "server1.example.com", Port: 443, Priority: 10, Weight: 100},
	}, nil)
	tp.ResolveSRV(context.Background())

	if tp.NeedsRefresh(time.Minute) {
		t.Error("Should not need refresh immediately after lookup")
	}

	// With very short TTL, should need refresh
	if !tp.NeedsRefresh(time.Nanosecond) {
		t.Error("Should need refresh with expired TTL")
	}
}
