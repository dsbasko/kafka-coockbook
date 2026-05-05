FROM golang:1.26-alpine AS build
WORKDIR /src
COPY main.go ./main.go
RUN go mod init mockwebhook \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mock-webhook .

FROM alpine:3.20
RUN apk add --no-cache wget ca-certificates
COPY --from=build /out/mock-webhook /usr/local/bin/mock-webhook
EXPOSE 8093
USER 1000
ENTRYPOINT ["/usr/local/bin/mock-webhook"]
