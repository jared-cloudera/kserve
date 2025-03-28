# Build the inference-agent binary
FROM golang:1.23 AS builder

# Copy in the go src
WORKDIR /go/src/github.com/kserve/kserve
COPY go.mod  go.mod
COPY go.sum  go.sum

RUN go mod download

COPY cmd/    cmd/
COPY pkg/    pkg/

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -a -o agent ./cmd/agent
RUN CGO_ENABLED=0 go install github.com/go-delve/delve/cmd/dlv@latest

# Copy the inference-agent into a thin image
#FROM gcr.io/distroless/static:nonroot
FROM golang:1.23
USER 1
COPY third_party/ third_party/
WORKDIR /ko-app
COPY --from=builder /go/src/github.com/kserve/kserve/agent /ko-app/
COPY --from=builder /go/bin/dlv /ko-app/
ENTRYPOINT ["/ko-app/dlv", "exec", "/ko-app/agent", "--headless", "--listen=:2345", "--api-version=2", "--accept-multiclient", "--"]

