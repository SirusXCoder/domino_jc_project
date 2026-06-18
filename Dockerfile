FROM golang:1.25-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/domino-server ./cmd/server

FROM alpine:3.21

RUN apk add --no-cache ca-certificates

COPY --from=builder /bin/domino-server /usr/local/bin/domino-server

EXPOSE 8080 50051 2112

ENTRYPOINT ["domino-server"]
