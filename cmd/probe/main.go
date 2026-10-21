// Package main runs a synthetic probe against the insurance-apis API from
// a fixed location (e.g. a home k8s cluster in Minneapolis) and exposes
// per-region health, latency, and OFAC-list-staleness metrics via
// Prometheus. It is designed to make a DR failover visible as a step
// change in `probe_served_region` and as a shift in `probe_duration_seconds`.
//
// The probe hits three targets concurrently each cycle:
//
//   - "public"  — the public DNS name (api.<domain>). Follows Route 53
//     failover routing; `served_region` here flips during a DR event.
//   - "primary" — the us-east-1 regional APIGW target domain, with the
//     public Host header. Always serves from us-east-1 regardless of
//     DNS. Used to isolate "primary region is up" from "DNS is pointing
//     at primary".
//   - "secondary" — same, for us-west-2.
//
// Each target is probed with net/http/httptrace to break out DNS lookup,
// TLS handshake, TTFB, and total duration. The response body is parsed
// to pull `data.region` (added to the API in nevadoai/insurance-apis#55),
// `data.ofacList.stale`, and `data.ofacList.listAgeHours`.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Target names us by convention: "public", "primary", "secondary".
// Changing these breaks the Grafana dashboard labels.
type target struct {
	name string
	// url is where the request actually goes. For the regional targets this
	// is the APIGW regional target domain, so Route 53 is bypassed.
	url string
	// host is the Host header to send. For the regional targets it must be
	// the public custom-domain name so APIGW can resolve the API mapping;
	// without it the regional endpoint 404s.
	host string
}

type healthBody struct {
	Data struct {
		Status   string `json:"status"`
		Region   string `json:"region"`
		OFACList *struct {
			Stale        bool    `json:"stale"`
			ListAgeHours float64 `json:"listAgeHours"`
		} `json:"ofacList"`
	} `json:"data"`
}

type metrics struct {
	up           *prometheus.GaugeVec
	httpStatus   *prometheus.GaugeVec
	servedRegion *prometheus.GaugeVec
	ofacStale    *prometheus.GaugeVec
	ofacListAge  *prometheus.GaugeVec
	// Durations are a histogram so the dashboard can show p50/p95/p99.
	duration *prometheus.HistogramVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		up: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "probe_up",
			Help: "1 if the last probe for this target returned a 2xx, 0 otherwise.",
		}, []string{"target"}),
		httpStatus: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "probe_http_status",
			Help: "HTTP status code of the last probe response (0 on connection error).",
		}, []string{"target"}),
		// The gauge is 1 for whichever region the last response actually came
		// from, 0 for all other regions. The dashboard reads this as a step
		// signal for failover events on the public target.
		servedRegion: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "probe_served_region",
			Help: "1 if `served_region` was the region that served the last successful probe; 0 otherwise.",
		}, []string{"target", "served_region"}),
		ofacStale: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "probe_ofac_list_stale",
			Help: "1 if the OFAC list reported stale by this target in its last probe, 0 otherwise, -1 if unknown.",
		}, []string{"target"}),
		ofacListAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "probe_ofac_list_age_seconds",
			Help: "Age of the OFAC list reported by this target in its last probe.",
		}, []string{"target"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "probe_duration_seconds",
			Help: "Probe HTTP duration split by phase.",
			// Buckets chosen for a home-ISP → us-east-1/us-west-2 round trip:
			// anything over 2s is clearly broken; most p99 should fit in 500ms.
			Buckets: []float64{0.025, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1, 2, 5, 10},
		}, []string{"target", "phase"}),
	}
	reg.MustRegister(m.up, m.httpStatus, m.servedRegion, m.ofacStale, m.ofacListAge, m.duration)
	return m
}

// knownRegions seeds the servedRegion vector so panels have the series even
// before the first probe hits that region. Keep in sync with the Terraform
// in nevado-insurance-apis.
var knownRegions = []string{"us-east-1", "us-west-2"}

type probe struct {
	targets []target
	timeout time.Duration
	client  *http.Client
	m       *metrics
	log     *slog.Logger
}

func newProbe(targets []target, timeout time.Duration, m *metrics, log *slog.Logger) *probe {
	tr := &http.Transport{
		// Fresh DNS each cycle so DNS failover shows up in probe_duration_seconds{phase=dns}.
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return &probe{
		targets: targets,
		timeout: timeout,
		client:  &http.Client{Transport: tr, Timeout: timeout},
		m:       m,
		log:     log,
	}
}

type phaseTimings struct {
	dnsStart, dnsDone     time.Time
	connStart, connDone   time.Time
	tlsStart, tlsDone     time.Time
	gotFirstByte          time.Time
	wroteRequest          time.Time
	requestStart, allDone time.Time
}

func (p *probe) runOnce(ctx context.Context, t target) {
	logger := p.log.With("target", t.name, "url", t.url)
	timings := &phaseTimings{requestStart: time.Now()}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	trace := &httptrace.ClientTrace{
		DNSStart:             func(httptrace.DNSStartInfo) { timings.dnsStart = time.Now() },
		DNSDone:              func(httptrace.DNSDoneInfo) { timings.dnsDone = time.Now() },
		ConnectStart:         func(string, string) { timings.connStart = time.Now() },
		ConnectDone:          func(string, string, error) { timings.connDone = time.Now() },
		TLSHandshakeStart:    func() { timings.tlsStart = time.Now() },
		TLSHandshakeDone:     func(tls.ConnectionState, error) { timings.tlsDone = time.Now() },
		WroteRequest:         func(httptrace.WroteRequestInfo) { timings.wroteRequest = time.Now() },
		GotFirstResponseByte: func() { timings.gotFirstByte = time.Now() },
	}
	ctx = httptrace.WithClientTrace(ctx, trace)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.url, nil)
	if err != nil {
		logger.Error("build request", "err", err)
		p.m.up.WithLabelValues(t.name).Set(0)
		p.m.httpStatus.WithLabelValues(t.name).Set(0)
		return
	}
	// For regional targets, Go's http.Request uses the URL's Host unless
	// Host is set explicitly. Setting Host here is what makes APIGW route to
	// the right custom-domain mapping.
	if t.host != "" {
		req.Host = t.host
	}

	resp, err := p.client.Do(req)
	timings.allDone = time.Now()
	if err != nil {
		logger.Warn("probe failed", "err", err)
		p.m.up.WithLabelValues(t.name).Set(0)
		p.m.httpStatus.WithLabelValues(t.name).Set(0)
		// Reset served_region: no region claim means no series lit for this target.
		p.resetServedRegion(t.name)
		p.recordDurations(t.name, timings)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	p.m.httpStatus.WithLabelValues(t.name).Set(float64(resp.StatusCode))
	up := 0.0
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		up = 1
	}
	p.m.up.WithLabelValues(t.name).Set(up)
	p.recordDurations(t.name, timings)

	var h healthBody
	if jerr := json.Unmarshal(body, &h); jerr != nil {
		logger.Warn("body not JSON (treating target as up/down from status only)", "err", jerr, "bodyPreview", preview(body))
		p.resetServedRegion(t.name)
		return
	}

	// servedRegion: light exactly one region label (the one that answered).
	// If the response is missing region, zero out all labels so stale series
	// don't linger. The field is required in OpenAPI; empty here means the
	// Lambda env didn't set AWS_REGION, which is a bug worth seeing.
	p.resetServedRegion(t.name)
	if h.Data.Region != "" {
		p.m.servedRegion.WithLabelValues(t.name, h.Data.Region).Set(1)
	}

	if h.Data.OFACList != nil {
		stale := -1.0
		if h.Data.OFACList.Stale {
			stale = 1
		} else {
			stale = 0
		}
		p.m.ofacStale.WithLabelValues(t.name).Set(stale)
		p.m.ofacListAge.WithLabelValues(t.name).Set(h.Data.OFACList.ListAgeHours * 3600)
	} else {
		p.m.ofacStale.WithLabelValues(t.name).Set(-1)
	}

	logger.Debug("probe ok",
		"status", resp.StatusCode,
		"served_region", h.Data.Region,
		"total_ms", timings.allDone.Sub(timings.requestStart).Milliseconds(),
	)
}

func (p *probe) resetServedRegion(target string) {
	for _, r := range knownRegions {
		p.m.servedRegion.WithLabelValues(target, r).Set(0)
	}
}

func (p *probe) recordDurations(target string, t *phaseTimings) {
	seconds := func(a, b time.Time) float64 {
		if a.IsZero() || b.IsZero() || !b.After(a) {
			return 0
		}
		return b.Sub(a).Seconds()
	}
	p.m.duration.WithLabelValues(target, "dns").Observe(seconds(t.dnsStart, t.dnsDone))
	p.m.duration.WithLabelValues(target, "connect").Observe(seconds(t.connStart, t.connDone))
	p.m.duration.WithLabelValues(target, "tls").Observe(seconds(t.tlsStart, t.tlsDone))
	// TTFB is from request-write-done to the first byte back: pure server-side
	// latency plus one network RTT.
	p.m.duration.WithLabelValues(target, "ttfb").Observe(seconds(t.wroteRequest, t.gotFirstByte))
	p.m.duration.WithLabelValues(target, "total").Observe(seconds(t.requestStart, t.allDone))
}

func preview(b []byte) string {
	s := string(b)
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}

func (p *probe) runLoop(ctx context.Context, interval time.Duration) {
	// Fire once immediately so metrics appear on startup, then on interval.
	p.sweep(ctx)
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.sweep(ctx)
		}
	}
}

func (p *probe) sweep(ctx context.Context) {
	var wg sync.WaitGroup
	for _, t := range p.targets {
		wg.Add(1)
		go func(t target) {
			defer wg.Done()
			p.runOnce(ctx, t)
		}(t)
	}
	wg.Wait()
}

type config struct {
	listenAddr string
	interval   time.Duration
	timeout    time.Duration
	targets    []target
	logLevel   slog.Level
}

func parseFlags() (config, error) {
	var (
		listen    = flag.String("listen", getEnv("LISTEN", ":8080"), "listen address for /metrics and /healthz")
		interval  = flag.Duration("interval", getEnvDuration("INTERVAL", 5*time.Second), "time between probe sweeps")
		timeout   = flag.Duration("timeout", getEnvDuration("TIMEOUT", 10*time.Second), "per-request timeout")
		publicURL = flag.String("public-url", getEnv("PUBLIC_URL", "https://api.insurance-apis.nevado.nevado.ai/v1/health"), "public endpoint (follows Route 53 failover)")
		primURL   = flag.String("primary-url", getEnv("PRIMARY_URL", "https://d-y3puyjgig3.execute-api.us-east-1.amazonaws.com/v1/health"), "us-east-1 APIGW regional target domain + /v1/health")
		secURL    = flag.String("secondary-url", getEnv("SECONDARY_URL", "https://d-vkjykdwoqe.execute-api.us-west-2.amazonaws.com/v1/health"), "us-west-2 APIGW regional target domain + /v1/health")
		host      = flag.String("host-header", getEnv("HOST_HEADER", "api.insurance-apis.nevado.nevado.ai"), "Host header to send on regional probes (APIGW routes by Host header)")
		logLevel  = flag.String("log-level", getEnv("LOG_LEVEL", "info"), "debug|info|warn|error")
	)
	flag.Parse()

	cfg := config{listenAddr: *listen, interval: *interval, timeout: *timeout}
	cfg.targets = []target{
		{name: "public", url: *publicURL},
		{name: "primary", url: *primURL, host: *host},
		{name: "secondary", url: *secURL, host: *host},
	}
	switch strings.ToLower(*logLevel) {
	case "debug":
		cfg.logLevel = slog.LevelDebug
	case "info":
		cfg.logLevel = slog.LevelInfo
	case "warn":
		cfg.logLevel = slog.LevelWarn
	case "error":
		cfg.logLevel = slog.LevelError
	default:
		return cfg, fmt.Errorf("invalid log-level %q", *logLevel)
	}
	return cfg, nil
}

func getEnv(k, def string) string {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		return v
	}
	return def
}

func getEnvDuration(k string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(k); ok && v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.logLevel}))

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)
	p := newProbe(cfg.targets, cfg.timeout, m, log)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// HTTP server for Prometheus + liveness.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	srv := &http.Server{Addr: cfg.listenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		log.Info("probe started",
			"listen", cfg.listenAddr,
			"interval", cfg.interval,
			"timeout", cfg.timeout,
			"targets", len(cfg.targets),
		)
		p.runLoop(ctx, cfg.interval)
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server exited", "err", err)
			cancel()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown requested")
	shutdownCtx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer scancel()
	_ = srv.Shutdown(shutdownCtx)
	log.Info("stopped")
}
