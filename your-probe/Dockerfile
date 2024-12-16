FROM golang:1.23.4 AS builder

ARG LDFLAGS
ENV LDFLAGS=${LDFLAGS}

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 go build -ldflags="${LDFLAGS}" -o dist/ ./...

FROM gcr.io/distroless/static-debian12
WORKDIR /app
USER nonroot:nonroot
COPY --from=builder --chown=nonroot:nonroot /src/dist /app/bin
ENTRYPOINT ["/app/bin/your-probe"]