FROM golang:1.26-alpine AS build
WORKDIR /src
COPY main.go ./main.go
RUN go mod init mockapns \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mock-apns .

FROM alpine:3.20
RUN apk add --no-cache wget ca-certificates
COPY --from=build /out/mock-apns /usr/local/bin/mock-apns
EXPOSE 8092
USER 1000
ENTRYPOINT ["/usr/local/bin/mock-apns"]
