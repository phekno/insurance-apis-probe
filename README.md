# insurance-apis-probe

A tiny Go daemon that probes [`insurance-apis`](https://github.com/nevadoai/nevado-insurance-apis-20261008-210439)
from a fixed location (my home k8s cluster in Minneapolis) and exposes
per-region health, latency, and OFAC-list-staleness metrics via Prometheus.

Built to make the first DR failover drill measurable: Minneapolis sits
roughly between `us-east-1` and `us-west-2`, so DNS failover shows up as
a visible step change in `probe_served_region` and a shift in
`probe_duration_seconds`.

See
[nevadoai/nevado-planning#87](https://github.com/nevadoai/nevado-planning/issues/87)
/ [PRO-37](https://linear.app/nevado-products/issue/PRO-37) for the DR
work this feeds into.

## What it does

Every N seconds (default 5), concurrently probes three targets and emits
metrics:

| Target | URL | Why |
|---|---|---|
| `public` | `https://api.insurance-apis.nevado.nevado.ai/v1/health` | Follows Route 53 failover routing; `served_region` here flips during a DR event |
| `primary` | `https://d-y3puyjgig3.execute-api.us-east-1.amazonaws.com/v1/health` + `Host: api.insurance-apis.nevado.nevado.ai` | Always hits us-east-1 directly; proves primary is up independent of DNS |
| `secondary` | `https://d-vkjykdwoqe.execute-api.us-west-2.amazonaws.com/v1/health` + `Host: api.insurance-apis.nevado.nevado.ai` | Always hits us-west-2 directly |

Uses `net/http/httptrace` to break out DNS lookup, TLS handshake, TTFB,
and total latency per request. Parses the response to pull
`data.region` (nevadoai/insurance-apis#55),
`data.ofacList.stale`, and `data.ofacList.listAgeHours`.

## Metrics (on `:8080/metrics`)

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `probe_up` | gauge | `target` | 1 if last probe got 2xx, else 0 |
| `probe_http_status` | gauge | `target` | HTTP status (0 on connection error) |
| `probe_served_region` | gauge | `target`, `served_region` | 1 for the region that answered the last request, 0 for others. **This is the failover signal on `target=public`** |
| `probe_ofac_list_stale` | gauge | `target` | 1 stale, 0 fresh, -1 unknown |
| `probe_ofac_list_age_seconds` | gauge | `target` | OFAC list age |
| `probe_duration_seconds` | histogram | `target`, `phase` | Phases: `dns`, `connect`, `tls`, `ttfb`, `total` |

Also exposes `/healthz` on the same port.

## Running

```bash
# Default: hits the production targets every 5s
go run ./cmd/probe

# All settings have env-var equivalents:
LISTEN=:8080 \
INTERVAL=5s \
TIMEOUT=10s \
PUBLIC_URL=https://api.insurance-apis.nevado.nevado.ai/v1/health \
PRIMARY_URL=https://d-y3puyjgig3.execute-api.us-east-1.amazonaws.com/v1/health \
SECONDARY_URL=https://d-vkjykdwoqe.execute-api.us-west-2.amazonaws.com/v1/health \
HOST_HEADER=api.insurance-apis.nevado.nevado.ai \
LOG_LEVEL=info \
go run ./cmd/probe
```

Flags take precedence over env vars.

## Container

```
ghcr.io/phekno/insurance-apis-probe:main    # rolling, built on push to main
ghcr.io/phekno/insurance-apis-probe:<sha>   # per-commit, pin by digest for GitOps
ghcr.io/phekno/insurance-apis-probe:vX.Y.Z  # semver tags
```

Multi-arch (`linux/amd64`, `linux/arm64`), distroless base, non-root
user. Target image size <20MB.

## Deployment

GitOps via Flux in [`phekno/home-ops`](https://github.com/phekno/home-ops):
`kubernetes/apps/observability/insurance-apis-probe/`.
