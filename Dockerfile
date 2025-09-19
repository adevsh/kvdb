# Dockerfile for key-value database REPL
# purpose: Containerize the distributed KVDB with Raft consensus

# Use the official Go Image as a base
FROM golang:1.25-alpine AS builder

# Install git (required for go modules in alpine)
RUN apk --no-cache add git

WORKDIR /app

COPY go.mod ./

RUN go mod download || true

COPY . .
RUN go build -ldflags="-s -w" -o kvdb ./cmd

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app/kvdb .
RUN mkdir -p /app/data

EXPOSE 9090

ENTRYPOINT [ "./kvdb" ]
CMD ["--raft", ":9090", "--data-dir", "/app/data"]