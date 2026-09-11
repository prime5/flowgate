FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /out/flowgate ./cmd/flowgate

FROM alpine:3.20
COPY --from=build /out/flowgate /flowgate
EXPOSE 8080
ENTRYPOINT ["/flowgate"]
