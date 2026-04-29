.PHONY: help proto-gen proto-lint \
        run-server run-client \
        grpcurl-list grpcurl-describe grpcurl-create grpcurl-get \
        build clean

GRPC_ADDR ?= localhost:50051

help:
	@echo "make proto-gen          - сгенерировать gen/orders/v1/{orders.pb.go,orders_grpc.pb.go}"
	@echo "make proto-lint         - buf lint"
	@echo ""
	@echo "make run-server         - запустить grpc-server на $(GRPC_ADDR)"
	@echo "make run-client         - запустить grpc-client (Create + Get + NotFound demo)"
	@echo ""
	@echo "make grpcurl-list       - перечислить сервисы через reflection"
	@echo "make grpcurl-describe   - описание метода Create"
	@echo "make grpcurl-create     - вручную дёрнуть Create через grpcurl"
	@echo "make grpcurl-get        - вручную дёрнуть Get (передай ID=<uuid>)"
	@echo ""
	@echo "make build              - go build ./..."
	@echo "make clean              - удалить gen/"

proto-gen:
	buf generate

proto-lint:
	buf lint

run-server:
	go run ./cmd/grpc-server -addr=$(GRPC_ADDR)

run-client:
	go run ./cmd/grpc-client -addr=$(GRPC_ADDR)

# grpcurl нужен отдельно — `brew install grpcurl`. Reflection включён,
# поэтому .proto-файл клиенту не нужен.
grpcurl-list:
	grpcurl -plaintext $(GRPC_ADDR) list

grpcurl-describe:
	grpcurl -plaintext $(GRPC_ADDR) describe orders.v1.OrderService.Create

grpcurl-create:
	grpcurl -plaintext -d '{"customer_id":"cus-007","amount_cents":2599,"currency":"EUR"}' \
	  $(GRPC_ADDR) orders.v1.OrderService/Create

# Передавай id явно: `make grpcurl-get ID=<uuid из ответа Create>`.
grpcurl-get:
	@if [ -z "$(ID)" ]; then \
	  echo "usage: make grpcurl-get ID=<uuid>"; exit 2; \
	fi
	grpcurl -plaintext -d '{"id":"$(ID)"}' \
	  $(GRPC_ADDR) orders.v1.OrderService/Get

build:
	go build ./...

clean:
	rm -rf gen
