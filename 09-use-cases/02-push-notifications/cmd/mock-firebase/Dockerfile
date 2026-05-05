FROM golang:1.26-alpine AS build
WORKDIR /src
COPY main.go ./main.go
RUN go mod init mockfirebase \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mock-firebase .

FROM alpine:3.20
RUN apk add --no-cache wget ca-certificates
COPY --from=build /out/mock-firebase /usr/local/bin/mock-firebase
EXPOSE 8091
USER 1000
ENTRYPOINT ["/usr/local/bin/mock-firebase"]
