# Multi-stage build mock-webhook'а. Файл main.go не зависит ни от чего
# кроме стандартной библиотеки, поэтому собираем отдельным go-модулем
# на лету — не нужно тащить в контейнер весь модуль лекции и replace
# на ../../internal.
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY main.go ./main.go
RUN go mod init mockwebhook \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mock-webhook .

FROM alpine:3.20
RUN apk add --no-cache wget ca-certificates
COPY --from=build /out/mock-webhook /usr/local/bin/mock-webhook
EXPOSE 8090
USER 1000
ENTRYPOINT ["/usr/local/bin/mock-webhook"]
