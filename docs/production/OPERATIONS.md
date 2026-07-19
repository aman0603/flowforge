# Operations Runbook (Phase 13)

Day-2 operations: monitoring, alerting, routine tasks, and incident response.

## Observability

- **Metrics:** Prometheus on `METRICS_ADDR` (`:9091/metrics`). Scraped by the
  bundled Prometheus; dashboards in Grafana.
- **Tracing:** OTLP → Jaeger when `OTEL_DISABLED=false`.
- **Logs:** structured JSON on stdout; ship via your log agent.

## Key Signals

| Signal | Source | Watch for |
|---|---|---|
| Task throughput / latency | metrics | Drops → worker or DB issue |
| Task retry / failure rate | metrics | Spikes → bad workflow or dependency outage |
| Outbox backlog | metrics | Growth → Kafka down or publisher stalled |
| Stale-task reclamations | recovery metrics | Elevated → worker crashes |
| DB pool in-use vs max | metrics | Saturation → raise pool or add replicas |
| gRPC error/retry rate | metrics | Unavailable → network/partition |
| `/readyz` status | probe | 503 → dependency outage |

## Suggested Alerts

- Outbox backlog > threshold for 5m (Kafka/publisher problem).
- `/readyz` failing on any instance for 2m.
- Task failure rate > baseline for 10m.
- DB pool in-use == max for 5m (connection exhaustion).
- Stale reclamations rising (worker instability).

## Routine Tasks

- **Scale workers:** `docker compose up --scale worker=N` or adjust replicas.
- **Outbox retention:** old published events pruned per `OUTBOX_RETENTION`.
- **Profiling:** set `PPROF_ENABLED=true` on one instance, capture, then
  disable. See PERFORMANCE.md.
- **Rotate TLS certs:** update cert/key files/secrets, restart instances
  one-by-one.

## Incident Response

1. Check `/readyz` across instances to isolate dependency vs app.
2. Inspect metrics/traces for the failing stage (claim, execute, publish).
3. Correlate with recent deploys or dependency alerts.
4. For dependency outages, rely on built-in recovery (see CHAOS.md) — the system
   pauses and resumes without data loss; avoid manual DB edits.
5. Capture a pprof profile if latency/CPU is the symptom.

## Graceful Restart

`SIGTERM` triggers context cancellation; in-flight work drains within the grace
period, leases expire, recovery reclaims. Safe to restart any instance anytime.
