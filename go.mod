module github.com/Bugs5382/go-saga-orchestration

go 1.26.2

require (
	github.com/go-chi/chi/v5 v5.3.0
	github.com/golang-migrate/migrate/v4 v4.19.1
	github.com/google/cel-go v0.28.1
	github.com/google/uuid v1.6.0
	github.com/gorilla/websocket v1.5.3
	github.com/jackc/pgx/v5 v5.10.0
	github.com/rabbitmq/amqp091-go v1.12.0
	github.com/rs/zerolog v1.35.1
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.11
)

require (
	cel.dev/expr v0.25.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/rogpeppe/go-internal v1.15.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
)

require (
	github.com/Bugs5382/go-saga-orchestration/clients/go/worker v0.0.0
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/jackc/pgerrcode v0.0.0-20220416144525-469b46aa5efa // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	golang.org/x/exp v0.0.0-20240823005443-9b4947da3948 // indirect
	golang.org/x/net v0.51.0 // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/sys v0.42.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260226221140-a57be14db171 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
)

replace github.com/Bugs5382/go-saga-orchestration/clients/go/worker => ./clients/go/worker
