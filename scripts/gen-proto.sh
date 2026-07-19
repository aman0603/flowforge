#!/usr/bin/env bash
# Regenerate Go code from the .proto contracts.
#
# Requires: protoc, protoc-gen-go, protoc-gen-go-grpc on PATH.
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
set -euo pipefail

cd "$(dirname "$0")/.."

protoc \
  --proto_path=proto \
  --go_out=internal/proto --go_opt=module=github.com/aman0603/flowforge/internal/proto \
  --go-grpc_out=internal/proto --go-grpc_opt=module=github.com/aman0603/flowforge/internal/proto \
  flowforge/common.proto flowforge/health.proto flowforge/scheduler.proto

echo "proto generated"
