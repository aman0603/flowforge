# --- Build Stage ---
FROM golang:1.26.5-alpine AS builder

WORKDIR /app

# Copy dependency manifests
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the application files
COPY . .

# Compile the application as a statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/flowforge cmd/flowforge/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/worker cmd/worker/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/publisher cmd/publisher/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/scheduler cmd/scheduler/main.go
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /app/recovery cmd/recovery/main.go

# --- Run Stage ---
FROM alpine:3.20

WORKDIR /app

# ca-certificates for TLS; wget (busybox) is used by the container healthcheck.
RUN apk --no-cache add ca-certificates wget

# Create a non-root user and group to run the service (least privilege).
RUN addgroup -S flowforge && adduser -S -G flowforge flowforge

# Copy compiled binary and the schema file from builder
COPY --from=builder /app/flowforge .
COPY --from=builder /app/worker .
COPY --from=builder /app/publisher .
COPY --from=builder /app/scheduler .
COPY --from=builder /app/recovery .
COPY --from=builder /app/schema.sql .

# Drop privileges.
RUN chown -R flowforge:flowforge /app
USER flowforge

# Expose HTTP port
EXPOSE 8080

# Liveness healthcheck for the API service (overridden per-service in compose
# where the process does not serve HTTP).
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
  CMD wget -q -O /dev/null http://localhost:8080/healthz || exit 1

# Run the app
CMD ["./flowforge"]
