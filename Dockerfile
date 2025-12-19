# syntax=docker/dockerfile:1

# =========================
# Stage 1: Build container
# =========================
FROM golang:1.23.2-alpine AS builder

WORKDIR /app

# Cache module dependencies first (improves rebuild speed)
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the project (source code, configs, etc.)
# This invalidates the cache only when source files change
COPY . .

# Build the Go binary (static executable)
RUN go build -o ingestor ./cmd/indexer

# =========================
# Stage 2: Runtime container
# =========================
FROM alpine:3.19

WORKDIR /app

# Install CA certificates (needed for HTTPS requests)
RUN apk add --no-cache ca-certificates

# Copy compiled binary from build stage
COPY --from=builder /app/ingestor /app/ingestor

# Default entrypoint: run the ingestor binary
ENTRYPOINT ["/app/ingestor"]
