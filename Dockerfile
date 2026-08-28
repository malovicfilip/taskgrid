FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o taskgrid .

FROM alpine:latest

RUN apk add --no-cache bash ca-certificates

WORKDIR /app

COPY --from=builder /app/taskgrid /app/taskgrid

EXPOSE 8080

CMD ["/app/taskgrid"]