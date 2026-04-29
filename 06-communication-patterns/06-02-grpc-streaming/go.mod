module github.com/dsbasko/kafka-sandbox/lectures/06-communication-patterns/06-02-grpc-streaming

go 1.26

require (
	github.com/dsbasko/kafka-sandbox/lectures/internal v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.76.0
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/net v0.52.0 // indirect
	golang.org/x/sys v0.43.0 // indirect
	golang.org/x/text v0.36.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250804133106-a7a43d27e69b // indirect
)

// internal — workspace member, локально через replace.
replace github.com/dsbasko/kafka-sandbox/lectures/internal => ../../internal
