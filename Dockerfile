FROM --platform=$BUILDPLATFORM golang:1.23 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
COPY proto/go.mod proto/go.sum ./proto/
RUN go mod download

COPY . .
RUN test "$TARGETARCH" = "amd64" && \
    CGO_ENABLED=0 GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
    go build -trimpath -ldflags="-s -w" -o /out/wrapper-manager .

FROM ubuntu:24.04

WORKDIR /root

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /out/wrapper-manager ./wrapper-manager

ENTRYPOINT ["./wrapper-manager"]
EXPOSE 8080
