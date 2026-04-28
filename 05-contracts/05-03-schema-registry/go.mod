module github.com/dsbasko/kafka-sandbox/lectures/05-contracts/05-03-schema-registry

go 1.26

require (
	github.com/dsbasko/kafka-sandbox/lectures/internal v0.0.0-00010101000000-000000000000
	github.com/twmb/franz-go v1.21.0
	github.com/twmb/franz-go/pkg/sr v1.7.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kadm v1.18.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.50.0 // indirect
)

// internal — workspace member, локально через replace.
replace github.com/dsbasko/kafka-sandbox/lectures/internal => ../../internal
