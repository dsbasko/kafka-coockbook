module github.com/dsbasko/kafka-sandbox/lectures/09-use-cases/01-microservices-comm

go 1.26

require (
	github.com/dsbasko/kafka-sandbox/lectures/internal v0.0.0-00010101000000-000000000000
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.7.2
	github.com/twmb/franz-go v1.21.0
	github.com/twmb/franz-go/pkg/kadm v1.18.0
	google.golang.org/grpc v1.76.0
	google.golang.org/protobuf v1.36.11
)

replace github.com/dsbasko/kafka-sandbox/lectures/internal => ../../internal

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.50.0 // indirect
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250804133106-a7a43d27e69b // indirect
)
