FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o taskgrid .

FROM alpine:latest

RUN apk add --no-cache bash ca-certificates \
    && addgroup -S -g 10001 taskgrid \
    && adduser -S -D -H -u 10001 -G taskgrid taskgrid

WORKDIR /app

COPY --from=builder --chown=10001:10001 /app/taskgrid /app/taskgrid

USER 10001:10001

EXPOSE 8080

CMD ["/app/taskgrid"]
