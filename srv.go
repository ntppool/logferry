package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"go.ntppool.org/common/logger"
)

// SRVTarget represents a single target from SRV record lookup.
type SRVTarget struct {
	Host     string
	Port     uint16
	Priority uint16
	Weight   uint16
}

// Address returns the host:port string for this target.
func (t SRVTarget) Address() string {
	return fmt.Sprintf("%s:%d", t.Host, t.Port)
}

// healthInfo tracks the health state of a single target.
type healthInfo struct {
	healthy       bool
	backoffUntil  time.Time
	backoffLevel  int // 0=5s, 1=10s, 2=20s, 3=40s, 4=80s, 5=160s, 6=300s
	lastError     error
	lastErrorTime time.Time
}

// backoffDurations defines the exponential backoff schedule.
// 5s -> 10s -> 20s -> 40s -> 80s -> 160s -> 300s (cap)
var backoffDurations = []time.Duration{
	5 * time.Second,
	10 * time.Second,
	20 * time.Second,
	40 * time.Second,
	80 * time.Second,
	160 * time.Second,
	300 * time.Second, // cap
}

// SRVMetrics holds Prometheus metrics for SRV target tracking.
type SRVMetrics struct {
	targetRequests *prometheus.CounterVec
	targetErrors   *prometheus.CounterVec
	targetHealthy  *prometheus.GaugeVec
	targetsTotal   prometheus.Gauge
	lookupsTotal   prometheus.Counter
	fallbackActive prometheus.Gauge
}

// NewSRVMetrics creates and registers SRV-related Prometheus metrics.
func NewSRVMetrics(reg prometheus.Registerer) *SRVMetrics {
	m := &SRVMetrics{
		targetRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "logferry_srv_target_requests_total",
			Help: "Requests sent to each SRV target",
		}, []string{"target"}),
		targetErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "logferry_srv_target_errors_total",
			Help: "Errors per SRV target",
		}, []string{"target"}),
		targetHealthy: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "logferry_srv_target_healthy",
			Help: "1 if target is healthy, 0 if in backoff",
		}, []string{"target"}),
		targetsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "logferry_srv_targets_total",
			Help: "Current number of SRV targets (0 if fallback mode)",
		}),
		lookupsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "logferry_srv_lookups_total",
			Help: "SRV DNS lookups performed",
		}),
		fallbackActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "logferry_srv_fallback_active",
			Help: "1 if using direct --upstream (no SRV)",
		}),
	}

	reg.MustRegister(m.targetRequests)
	reg.MustRegister(m.targetErrors)
	reg.MustRegister(m.targetHealthy)
	reg.MustRegister(m.targetsTotal)
	reg.MustRegister(m.lookupsTotal)
	reg.MustRegister(m.fallbackActive)

	return m
}

// TargetPool manages a pool of SRV targets with health tracking.
type TargetPool struct {
	mu sync.RWMutex

	// Configuration
	upstreamURL *url.URL // Original --upstream URL for fallback
	srvDomain   string   // Domain for SRV lookup (e.g., "ntppool.net")

	// State
	targets       []SRVTarget
	healthState   map[string]*healthInfo // "host:port" -> state
	activeTargets map[string]int         // "host:port" -> refresh cycles since last seen (for cleanup)
	fallbackMode  bool                   // True if using direct upstream
	lastLookup    time.Time              // Time of last SRV lookup

	// Metrics
	metrics *SRVMetrics

	// For testing: allow injecting a custom resolver
	resolver func(ctx context.Context, service, proto, name string) (string, []*net.SRV, error)
}

// NewTargetPool creates a new target pool for the given upstream URL.
func NewTargetPool(upstreamURL *url.URL, metrics *SRVMetrics) *TargetPool {
	srvDomain := deriveSRVDomain(upstreamURL.Hostname())

	tp := &TargetPool{
		upstreamURL:   upstreamURL,
		srvDomain:     srvDomain,
		healthState:   make(map[string]*healthInfo),
		activeTargets: make(map[string]int),
		fallbackMode:  true, // Start in fallback mode until first successful lookup
		metrics:       metrics,
		resolver:      defaultResolver,
	}

	if metrics != nil {
		metrics.fallbackActive.Set(1)
		metrics.targetsTotal.Set(0)
	}

	return tp
}

// defaultResolver uses the standard library DNS resolver.
func defaultResolver(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
	return net.DefaultResolver.LookupSRV(ctx, service, proto, name)
}

// deriveSRVDomain extracts the domain for SRV lookup from a hostname.
// For "logferry-api.ntppool.net" returns "ntppool.net"
// For "api.logs.example.com" returns "logs.example.com"
func deriveSRVDomain(hostname string) string {
	parts := strings.Split(hostname, ".")
	if len(parts) <= 2 {
		// Already a simple domain like "example.com" or single label
		return hostname
	}
	// Strip the first label
	return strings.Join(parts[1:], ".")
}

// ResolveSRV performs SRV lookup and updates the target pool.
// Returns true if targets were found, false if falling back to direct upstream.
func (tp *TargetPool) ResolveSRV(ctx context.Context) bool {
	log := logger.Setup()

	if tp.metrics != nil {
		tp.metrics.lookupsTotal.Inc()
	}

	srvName := fmt.Sprintf("_logferry._tcp.%s", tp.srvDomain)
	log.Debug("performing SRV lookup", "name", srvName)

	_, addrs, err := tp.resolver(ctx, "logferry", "tcp", tp.srvDomain)
	if err != nil {
		log.Info("SRV lookup failed, using fallback", "domain", tp.srvDomain, "err", err)
		tp.setFallbackMode(true)
		return false
	}

	if len(addrs) == 0 {
		log.Info("SRV lookup returned no targets, using fallback", "domain", tp.srvDomain)
		tp.setFallbackMode(true)
		return false
	}

	// Convert to our target type
	targets := make([]SRVTarget, 0, len(addrs))
	for _, addr := range addrs {
		// Remove trailing dot from hostname if present
		host := strings.TrimSuffix(addr.Target, ".")
		targets = append(targets, SRVTarget{
			Host:     host,
			Port:     addr.Port,
			Priority: addr.Priority,
			Weight:   addr.Weight,
		})
	}

	changed := tp.updateTargets(targets)
	if changed {
		log.Info("SRV lookup succeeded", "domain", tp.srvDomain, "targets", len(targets))
	} else {
		log.Debug("SRV lookup unchanged", "domain", tp.srvDomain, "targets", len(targets))
	}
	return true
}

// staleHealthCleanupCycles is the number of refresh cycles after which
// health state for removed targets is cleaned up.
const staleHealthCleanupCycles = 3

// updateTargets updates the target list while preserving health state.
// Returns true if the target set changed (additions, removals, or weight/priority changes).
func (tp *TargetPool) updateTargets(targets []SRVTarget) bool {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	oldTargets := make(map[string]SRVTarget)
	for _, t := range tp.targets {
		oldTargets[t.Address()] = t
	}

	newTargets := make(map[string]SRVTarget)
	for _, t := range targets {
		addr := t.Address()
		newTargets[addr] = t
		// Reset staleness counter for active targets
		tp.activeTargets[addr] = 0
		// Initialize health state for new targets
		if _, exists := tp.healthState[addr]; !exists {
			tp.healthState[addr] = &healthInfo{healthy: true}
			if tp.metrics != nil {
				tp.metrics.targetHealthy.WithLabelValues(addr).Set(1)
			}
		}
	}

	// Detect changes: added, removed, or weight/priority changed
	changed := len(oldTargets) != len(newTargets)
	if !changed {
		for addr, newT := range newTargets {
			oldT, exists := oldTargets[addr]
			if !exists || oldT.Weight != newT.Weight || oldT.Priority != newT.Priority {
				changed = true
				break
			}
		}
	}

	if changed {
		// Calculate weight percentages per priority group
		weightPcts := tp.calcWeightPcts(targets)

		log := logger.Setup()
		for addr, target := range newTargets {
			if _, exists := oldTargets[addr]; !exists {
				log.Info("SRV target added", "target", addr, "priority", target.Priority, "weight_pct", weightPcts[addr])
			}
		}
		for addr := range oldTargets {
			if _, exists := newTargets[addr]; !exists {
				log.Info("SRV target removed", "target", addr)
			}
		}
	}

	// Increment staleness counter for targets not in current set and clean up stale ones
	for addr := range tp.activeTargets {
		if _, exists := newTargets[addr]; !exists {
			tp.activeTargets[addr]++
			if tp.activeTargets[addr] >= staleHealthCleanupCycles {
				delete(tp.healthState, addr)
				delete(tp.activeTargets, addr)
				log := logger.Setup()
				log.Debug("cleaned up stale target health state", "target", addr)
			}
		}
	}

	tp.targets = targets
	tp.fallbackMode = false
	tp.lastLookup = time.Now()

	if tp.metrics != nil {
		tp.metrics.fallbackActive.Set(0)
		tp.metrics.targetsTotal.Set(float64(len(targets)))
	}

	return changed
}

// calcWeightPcts calculates the weight percentage for each target within its priority group.
// Weight 0 is treated as 1 per RFC 2782.
func (tp *TargetPool) calcWeightPcts(targets []SRVTarget) map[string]int {
	// Group by priority and sum effective weights
	type groupInfo struct {
		totalWeight uint32
	}
	groups := make(map[uint16]*groupInfo)
	for _, t := range targets {
		g, exists := groups[t.Priority]
		if !exists {
			g = &groupInfo{}
			groups[t.Priority] = g
		}
		w := t.Weight
		if w == 0 {
			w = 1
		}
		g.totalWeight += uint32(w)
	}

	pcts := make(map[string]int, len(targets))
	for _, t := range targets {
		w := t.Weight
		if w == 0 {
			w = 1
		}
		total := groups[t.Priority].totalWeight
		pcts[t.Address()] = int(uint32(w) * 100 / total)
	}
	return pcts
}

// setFallbackMode switches to/from fallback mode.
func (tp *TargetPool) setFallbackMode(fallback bool) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if tp.fallbackMode == fallback {
		return
	}

	tp.fallbackMode = fallback
	tp.lastLookup = time.Now()

	if fallback {
		tp.targets = nil
		if tp.metrics != nil {
			tp.metrics.fallbackActive.Set(1)
			tp.metrics.targetsTotal.Set(0)
		}
	}
}

// SelectTarget returns the URL for the next target to use.
// This implements full SRV priority/weight semantics.
func (tp *TargetPool) SelectTarget() string {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	if tp.fallbackMode || len(tp.targets) == 0 {
		return tp.upstreamURL.String()
	}

	target := tp.selectWeightedTarget()
	if target == nil {
		// All targets unhealthy, use fallback
		return tp.upstreamURL.String()
	}

	// Build URL with target host:port
	u := &url.URL{
		Scheme: "https",
		Host:   target.Address(),
		Path:   "/",
	}
	return u.String()
}

// selectWeightedTarget implements SRV priority and weight-based selection.
// Must be called with at least read lock held.
func (tp *TargetPool) selectWeightedTarget() *SRVTarget {
	now := time.Now()

	// Group targets by priority
	priorityGroups := make(map[uint16][]SRVTarget)
	for _, t := range tp.targets {
		priorityGroups[t.Priority] = append(priorityGroups[t.Priority], t)
	}

	// Get sorted priority levels (lower = higher priority)
	priorities := make([]uint16, 0, len(priorityGroups))
	for p := range priorityGroups {
		priorities = append(priorities, p)
	}
	slices.Sort(priorities)

	// Try each priority group in order
	for _, priority := range priorities {
		group := priorityGroups[priority]
		target := tp.selectFromGroup(group, now)
		if target != nil {
			return target
		}
	}

	// All targets unhealthy - pick the one with shortest remaining backoff
	return tp.selectLeastUnhealthy(now)
}

// selectFromGroup selects a target from a priority group using weight distribution.
// Returns nil if all targets in the group are unhealthy.
func (tp *TargetPool) selectFromGroup(group []SRVTarget, now time.Time) *SRVTarget {
	// Filter to healthy targets and calculate total weight
	var healthyTargets []SRVTarget
	var totalWeight uint32

	for _, t := range group {
		info := tp.healthState[t.Address()]
		if info != nil && !info.healthy && now.Before(info.backoffUntil) {
			continue // Skip unhealthy targets still in backoff
		}
		healthyTargets = append(healthyTargets, t)
		// Weight 0 is treated as weight 1 per RFC 2782
		weight := t.Weight
		if weight == 0 {
			weight = 1
		}
		totalWeight += uint32(weight)
	}

	if len(healthyTargets) == 0 {
		return nil
	}

	if len(healthyTargets) == 1 {
		return &healthyTargets[0]
	}

	// Weighted random selection per RFC 2782
	r := rand.Uint32N(totalWeight)
	var cumWeight uint32
	for i := range healthyTargets {
		weight := healthyTargets[i].Weight
		if weight == 0 {
			weight = 1
		}
		cumWeight += uint32(weight)
		if r < cumWeight {
			return &healthyTargets[i]
		}
	}

	// Shouldn't reach here, but return last target as fallback
	return &healthyTargets[len(healthyTargets)-1]
}

// selectLeastUnhealthy picks the target with the shortest remaining backoff time.
func (tp *TargetPool) selectLeastUnhealthy(now time.Time) *SRVTarget {
	if len(tp.targets) == 0 {
		return nil
	}

	var best *SRVTarget
	var bestBackoff time.Duration = time.Duration(1<<63 - 1) // max duration

	for i := range tp.targets {
		t := &tp.targets[i]
		info := tp.healthState[t.Address()]
		if info == nil {
			return t // No health info means healthy
		}

		remaining := info.backoffUntil.Sub(now)
		if remaining <= 0 {
			return t // Backoff expired
		}

		if remaining < bestBackoff {
			bestBackoff = remaining
			best = t
		}
	}

	return best
}

// MarkHealthy marks a target as healthy and resets its backoff.
func (tp *TargetPool) MarkHealthy(targetAddr string) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	info, exists := tp.healthState[targetAddr]
	if !exists {
		return
	}

	if !info.healthy {
		log := logger.Setup()
		log.Info("SRV target recovered", "target", targetAddr)
	}

	info.healthy = true
	info.backoffLevel = 0
	info.backoffUntil = time.Time{}
	info.lastError = nil

	if tp.metrics != nil {
		tp.metrics.targetHealthy.WithLabelValues(targetAddr).Set(1)
	}
}

// MarkUnhealthy marks a target as unhealthy and starts/increases backoff.
func (tp *TargetPool) MarkUnhealthy(targetAddr string, err error) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	info, exists := tp.healthState[targetAddr]
	if !exists {
		// Create new health state for unknown target (shouldn't happen normally)
		info = &healthInfo{}
		tp.healthState[targetAddr] = info
	}

	wasHealthy := info.healthy
	info.healthy = false
	info.lastError = err
	info.lastErrorTime = time.Now()

	// Calculate backoff duration
	backoff := backoffDurations[info.backoffLevel]
	info.backoffUntil = time.Now().Add(backoff)

	// Increase backoff level for next time (with cap)
	if info.backoffLevel < len(backoffDurations)-1 {
		info.backoffLevel++
	}

	if wasHealthy {
		log := logger.Setup()
		log.Warn("SRV target marked unhealthy", "target", targetAddr, "backoff", backoff, "err", err)
	}

	if tp.metrics != nil {
		tp.metrics.targetHealthy.WithLabelValues(targetAddr).Set(0)
		tp.metrics.targetErrors.WithLabelValues(targetAddr).Inc()
	}
}

// RecordRequest increments the request counter for a target.
func (tp *TargetPool) RecordRequest(targetAddr string) {
	if tp.metrics != nil {
		tp.metrics.targetRequests.WithLabelValues(targetAddr).Inc()
	}
}

// IsFallbackMode returns true if using direct upstream (no SRV).
func (tp *TargetPool) IsFallbackMode() bool {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return tp.fallbackMode
}

// NeedsRefresh returns true if the SRV TTL has expired and re-resolution is needed.
func (tp *TargetPool) NeedsRefresh(ttl time.Duration) bool {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	if tp.lastLookup.IsZero() {
		return true
	}

	return time.Since(tp.lastLookup) >= ttl
}

// TargetCount returns the current number of SRV targets (0 in fallback mode).
func (tp *TargetPool) TargetCount() int {
	tp.mu.RLock()
	defer tp.mu.RUnlock()
	return len(tp.targets)
}

// GetTargetAddress extracts the host:port from a target URL for health tracking.
func GetTargetAddress(targetURL string) string {
	u, err := url.Parse(targetURL)
	if err != nil {
		return ""
	}
	host := u.Host
	if host != "" && !strings.Contains(host, ":") {
		switch u.Scheme {
		case "https":
			host += ":443"
		case "http":
			host += ":80"
		}
	}
	return host
}
