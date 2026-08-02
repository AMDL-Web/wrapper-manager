module github.com/AMDL-Web/wrapper-manager

go 1.26.0

toolchain go1.26.5

require (
	github.com/artdarek/go-unzip v1.0.0
	github.com/creack/pty v1.1.24
	github.com/gofrs/uuid/v5 v5.5.0
	github.com/sirupsen/logrus v1.9.4
	golang.org/x/sync v0.22.0
	google.golang.org/grpc v1.83.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/AMDL-Web/wrapper-manager/proto v0.1.0
	github.com/hashicorp/golang-lru/v2 v2.0.7
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260729162451-8efbd57d26e0 // indirect
)

replace github.com/AMDL-Web/wrapper-manager/proto => ./proto
