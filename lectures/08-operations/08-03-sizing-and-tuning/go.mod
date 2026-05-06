module github.com/dsbasko/kafka-sandbox/lectures/08-operations/08-03-sizing-and-tuning

go 1.26

require (
	github.com/dsbasko/kafka-sandbox/lectures/internal v0.0.0-00010101000000-000000000000
	github.com/twmb/franz-go v1.21.0
	github.com/twmb/franz-go/pkg/kadm v1.18.0
)

require (
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/pierrec/lz4/v4 v4.1.26 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.13.1 // indirect
	golang.org/x/crypto v0.50.0 // indirect
)

replace github.com/dsbasko/kafka-sandbox/lectures/internal => ../../internal
