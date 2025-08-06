# Stage 1: Build the application
FROM golang:1.24-alpine AS build

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN go build -o main .

# Stage 2: Create the final image
FROM alpine:latest as prod

WORKDIR /root/
COPY --from=build /app/main .
EXPOSE 8080

CMD ["./main"]