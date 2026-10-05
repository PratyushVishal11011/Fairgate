FROM golang:1.27.1-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/fairgate ./cmd/gateway

FROM alpine:3.22

RUN apk --no-cache add ca-certificates
WORKDIR /app

COPY --from=build /out/fairgate /usr/local/bin/fairgate

EXPOSE 9001
ENTRYPOINT ["/usr/local/bin/fairgate"]
