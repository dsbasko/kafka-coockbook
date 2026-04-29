.PHONY: help proto-gen proto-lint topic-create topic-delete \
        run-grpc-listener-1 run-grpc-listener-2 run-grpc-listener-3 \
        run-grpc-listeners-3 run-grpc-broadcast run-grpc-broadcast-parallel \
        run-kafka-listener-analytics run-kafka-listener-notifications run-kafka-listener-billing \
        run-kafka-listeners-3 run-kafka-broadcast \
        build clean

BOOTSTRAP    ?= localhost:19092,localhost:19093,localhost:19094
TOPIC        ?= lecture-06-03-user-events
USERS        ?= 5

GRPC_ADDR_1  ?= localhost:50061
GRPC_ADDR_2  ?= localhost:50062
GRPC_ADDR_3  ?= localhost:50063
LISTENER_URLS ?= $(GRPC_ADDR_1),$(GRPC_ADDR_2),$(GRPC_ADDR_3)

help:
	@echo "make proto-gen                       - сгенерировать gen/users/v1/{users.pb.go,users_grpc.pb.go}"
	@echo "make proto-lint                      - buf lint"
	@echo ""
	@echo "make topic-create                    - создать $(TOPIC) (3 партиции, RF=3)"
	@echo "make topic-delete                    - удалить $(TOPIC)"
	@echo ""
	@echo "gRPC fan-out (sender знает URL'ы получателей):"
	@echo "  make run-grpc-listener-1           - listener analytics на $(GRPC_ADDR_1)"
	@echo "  make run-grpc-listener-2           - listener notifications на $(GRPC_ADDR_2)"
	@echo "  make run-grpc-listener-3           - listener billing на $(GRPC_ADDR_3)"
	@echo "  make run-grpc-listeners-3          - подсказка как запустить три приёмника в трёх терминалах"
	@echo "  make run-grpc-broadcast            - sender дёргает все три URL'а по очереди, USERS=$(USERS)"
	@echo "  make run-grpc-broadcast-parallel   - то же самое, но с -parallel=true"
	@echo ""
	@echo "Kafka pub/sub (sender ничего не знает о получателях):"
	@echo "  make run-kafka-listener-analytics      - consumer группа analytics"
	@echo "  make run-kafka-listener-notifications  - consumer группа notifications"
	@echo "  make run-kafka-listener-billing        - consumer группа billing"
	@echo "  make run-kafka-listeners-3             - подсказка как запустить три приёмника"
	@echo "  make run-kafka-broadcast               - sender пишет одно сообщение в $(TOPIC)"
	@echo ""
	@echo "make build                           - go build ./..."
	@echo "make clean                           - удалить gen/"

proto-gen:
	buf generate

proto-lint:
	buf lint

topic-create:
	docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 \
	  --create --if-not-exists \
	  --topic $(TOPIC) --partitions 3 --replication-factor 3

topic-delete:
	docker exec -it kafka-1 /opt/kafka/bin/kafka-topics.sh \
	  --bootstrap-server kafka-1:9092 \
	  --delete --topic $(TOPIC)

# --- gRPC fan-out ---

run-grpc-listener-1:
	LISTENER_NAME=analytics      go run ./cmd/grpc-listener -addr=$(GRPC_ADDR_1) -name=analytics

run-grpc-listener-2:
	LISTENER_NAME=notifications  go run ./cmd/grpc-listener -addr=$(GRPC_ADDR_2) -name=notifications

run-grpc-listener-3:
	LISTENER_NAME=billing        go run ./cmd/grpc-listener -addr=$(GRPC_ADDR_3) -name=billing

run-grpc-listeners-3:
	@echo "Запусти в трёх отдельных терминалах:"
	@echo "  make run-grpc-listener-1"
	@echo "  make run-grpc-listener-2"
	@echo "  make run-grpc-listener-3"
	@echo "Потом в четвёртом — make run-grpc-broadcast"

run-grpc-broadcast:
	go run ./cmd/grpc-broadcast -targets=$(LISTENER_URLS) -users=$(USERS)

run-grpc-broadcast-parallel:
	go run ./cmd/grpc-broadcast -targets=$(LISTENER_URLS) -users=$(USERS) -parallel=true

# --- Kafka pub/sub ---

run-kafka-listener-analytics:
	go run ./cmd/kafka-listener -group=analytics

run-kafka-listener-notifications:
	go run ./cmd/kafka-listener -group=notifications

run-kafka-listener-billing:
	go run ./cmd/kafka-listener -group=billing

run-kafka-listeners-3:
	@echo "Запусти в трёх отдельных терминалах:"
	@echo "  make run-kafka-listener-analytics"
	@echo "  make run-kafka-listener-notifications"
	@echo "  make run-kafka-listener-billing"
	@echo "Потом в четвёртом — make run-kafka-broadcast"

run-kafka-broadcast:
	go run ./cmd/kafka-broadcast -users=$(USERS)

build:
	go build ./...

clean:
	rm -rf gen
