FROM golang:1.24.3-alpine AS builder

WORKDIR /server

COPY go.mod ./

RUN go mod tidy

COPY server/ ./

RUN go build -o /app/server .

FROM alpine:3.18

WORKDIR /server

COPY --from=builder /app/server .

EXPOSE 6379

CMD ["./server"]


