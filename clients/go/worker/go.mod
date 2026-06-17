module github.com/Bugs5382/go-saga-orchestration/clients/go/worker

go 1.26.2

require (
	github.com/rabbitmq/amqp091-go v1.10.0
	github.com/rs/zerolog v1.35.1
	github.com/Bugs5382/go-saga-orchestration v0.0.0
	google.golang.org/grpc v1.74.2
)

require (
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sys v0.38.0 // indirect
	golang.org/x/text v0.31.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250818200422-3122310a409c // indirect
	google.golang.org/protobuf v1.36.7 // indirect
)

replace github.com/Bugs5382/go-saga-orchestration => ../../../
