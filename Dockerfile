# syntax=docker/dockerfile:1.7
#
# Multi-stage: build on golang:1.26-alpine, ship on distroless/static.
# Target image <20MB, non-root, no shell. Multi-arch (amd64 + arm64) when
# driven by `docker buildx` in CI.

FROM golang:1.26-alpine AS build
WORKDIR /src

# Separate copy of go.mod/go.sum so dep resolution caches across builds.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY cmd ./cmd

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
        -trimpath \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/probe \
        ./cmd/probe

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/probe /probe
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/probe"]
