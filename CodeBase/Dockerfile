FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /codebase .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /codebase /usr/local/bin/codebase
COPY codebase.docker.toml /usr/local/bin/codebase.toml

ENTRYPOINT ["codebase"]
