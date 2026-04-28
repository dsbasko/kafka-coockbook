module github.com/dsbasko/kafka-sandbox/lectures/05-contracts/05-01-why-contracts-and-wire-formats

go 1.26

require (
	github.com/dsbasko/kafka-sandbox/lectures/internal v0.0.0-00010101000000-000000000000
	github.com/hamba/avro/v2 v2.31.0
	github.com/twmb/franz-go v1.21.0
	github.com/twmb/franz-go/pkg/kadm v1.18.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.50.0 // indirect
)

// internal — workspace member, локально через replace.
replace github.com/dsbasko/kafka-sandbox/lectures/internal => ../../internal
