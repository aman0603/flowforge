# System Diagrams

Rendered architecture diagrams. Mermaid blocks render on GitHub; ASCII fallbacks
are included for plain-text viewers.

## Overall System (Mermaid)

```mermaid
flowchart TB
  client([Clients])
  api[API / REST]
  sched[Scheduler gRPC]
  recov[Recovery gRPC]
  worker[Workers stateless]
  pub[Publisher]
  consumer[Consumers]
  pg[(PostgreSQL)]
  redis[(Redis)]
  kafka[(Kafka)]
  prom[Prometheus]
  graf[Grafana]
  jaeger[Jaeger]

  client -->|REST| api
  api -->|state + outbox one TX| pg
  worker -->|ClaimTasks gRPC| sched
  worker -->|RecoverTask gRPC| recov
  sched --> pg
  recov --> pg
  worker -->|claim/execute/persist| pg
  worker -->|heartbeat + lease| redis
  pub -->|poll| pg
  pub -->|publish| kafka
  kafka --> consumer
  api & sched & recov & worker & pub -.metrics.-> prom
  prom --> graf
  api & sched & recov & worker & pub -.OTLP.-> jaeger
```

## Request/Data Flow (Layered)

```
        EXTERNAL                 INTERNAL (sync)            ASYNC
  ┌──────────────────┐     ┌──────────────────────┐   ┌─────────────┐
  │  Clients (REST)  │     │  Scheduler / Recovery │   │   Consumers │
  └────────┬─────────┘     │      (gRPC)           │   └──────▲──────┘
           │               └──────────┬────────────┘          │
           ▼                          │                        │
  ┌──────────────────┐               │                 ┌──────┴──────┐
  │   API (:8080)    │               │                 │   Kafka     │
  └────────┬─────────┘               │                 └──────▲──────┘
           │ TX (state+outbox)       │ claim/recover          │ publish
           ▼                          ▼                        │
  ┌───────────────────────────────────────────┐        ┌──────┴──────┐
  │            PostgreSQL (truth)              │◄───────│  Publisher  │
  └───────────────────────────────────────────┘  poll  └─────────────┘
           ▲                          ▲
   leases/ │                          │ claim/execute/persist
 heartbeat │                   ┌──────┴──────┐
     ┌─────┴─────┐             │   Workers   │
     │   Redis   │◄────────────┤ (stateless) │
     └───────────┘             └─────────────┘
```

## Deployment Topology (Docker Compose)

```mermaid
flowchart LR
  subgraph infra[Infrastructure]
    db[(postgres:16)]
    redis[(redis:7)]
    kafka[(cp-kafka KRaft)]
  end
  subgraph app[FlowForge services]
    a[app / API]
    s[scheduler]
    r[recovery]
    w[worker xN]
    p[publisher]
  end
  subgraph obs[Observability]
    prom[prometheus]
    graf[grafana]
    jae[jaeger]
  end
  a --> db
  s --> db
  r --> db
  w --> db & redis
  w --> s & r
  p --> db & kafka
  prom --> a & s & r & w & p
  graf --> prom
  a & s & r & w & p --> jae
```

## Component Ownership

```
┌─────────────┬──────────────────────────────────────────────┐
│  Component  │  Owns                                          │
├─────────────┼──────────────────────────────────────────────┤
│ REST API    │  External interface (create/query)            │
│ gRPC        │  Synchronous internal comms (claim/recover)    │
│ Kafka       │  Asynchronous event stream                     │
│ Redis       │  Ephemeral coordination (leases/heartbeats)    │
│ PostgreSQL  │  Durable state (source of truth)               │
│ Workers     │  Task execution (stateless)                    │
│ Publisher   │  Outbox → Kafka relay (no state mutation)      │
│ Scheduler   │  Claim + retry promotion (no execution)        │
│ Recovery    │  Stale reclaim (no execution)                  │
└─────────────┴──────────────────────────────────────────────┘
```
