# Stage 1: Build the application
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o api ./main.go

# Stage 2: Create the final image
FROM alpine:3.20 AS prod
RUN apk add --no-cache curl
WORKDIR /root/
COPY --from=builder /app/api /api
EXPOSE 8080
CMD ["/api"]