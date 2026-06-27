# syntax=docker/dockerfile:1
# Multi-stage, multi-service build. Pass --build-arg SERVICE=api|engine to pick
# which cmd/ binary to compile. Cross-compiled on the native build platform
# (buildx $BUILDPLATFORM) for fast linux/amd64 + linux/arm64 images.
ARG GO_VERSION=1.26.2

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build
ARG SERVICE
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN test -n "$SERVICE" || { echo "SERVICE build-arg is required (api|engine)"; exit 1; }
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
