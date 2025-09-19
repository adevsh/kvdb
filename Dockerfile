# Dockerfile for key-value database REPL
# purpose: Containerize the distributed KVDB with Raft consensus

# Use the official Go Image as a base
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod ./

RUN go mod download

COPY . .
RUN go build -o kvdb ./cmd/kvdb

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app/kvdb .
RUN mkdir -p /app/data

EXPOSE 8080 9090

ENTRYPOINT [ "./kvdb" ]
CMD ["--http", ":8080", "--raft", ":9090", "--data-dir", "/app/data"]