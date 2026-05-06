.PHONY: help proto-gen proto-lint \
        run-server run-subscribe run-upload run-chat \
        build clean

GRPC_ADDR ?= localhost:50052

help:
	@echo "make proto-gen          - сгенерировать gen/orders/v1/{streaming.pb.go,streaming_grpc.pb.go}"
	@echo "make proto-lint         - buf lint"
	@echo ""
	@echo "make run-server         - запустить streaming-server на $(GRPC_ADDR)"
	@echo "make run-subscribe      - server-stream клиент: подпишется на 5 событий и выйдет"
	@echo "make run-upload         - client-stream клиент: 20 OrderInput'ов, 3 невалидных"
	@echo "make run-chat           - bidi клиент: 5 ping, читает echo с throttling от сервера"
	@echo ""
	@echo "make build              - go build ./..."
	@echo "make clean              - удалить gen/"

proto-gen:
	buf generate

proto-lint:
	buf lint

run-server:
	go run ./cmd/server -addr=$(GRPC_ADDR)

run-subscribe:
	go run ./cmd/client-server-stream -addr=$(GRPC_ADDR) -limit=5

run-upload:
	go run ./cmd/client-upload -addr=$(GRPC_ADDR) -count=20 -bad=3

run-chat:
	go run ./cmd/client-bidi -addr=$(GRPC_ADDR) -count=5 -interval=50ms

build:
	go build ./...

clean:
	rm -rf gen
