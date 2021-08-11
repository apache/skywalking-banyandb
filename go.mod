module github.com/apache/skywalking-banyandb

go 1.16

require (
	github.com/RoaringBitmap/roaring v0.9.1
	github.com/cespare/xxhash v1.1.0
	github.com/dgraph-io/badger/v3 v3.2011.1
	github.com/golang/mock v1.5.0
	github.com/golang/protobuf v1.5.2
	github.com/google/go-cmp v0.5.6
	github.com/google/uuid v1.3.0
	github.com/klauspost/compress v1.13.1 // indirect
	github.com/oklog/run v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/rs/zerolog v1.23.0
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.7.0
	go.uber.org/multierr v1.7.0
	golang.org/x/net v0.0.0-20210716203947-853a461950ff // indirect
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	google.golang.org/genproto v0.0.0-20210722135532-667f2b7c528f // indirect
	google.golang.org/grpc v1.39.0
	google.golang.org/protobuf v1.27.1
)

replace github.com/dgraph-io/badger/v3 v3.2011.1 => github.com/SkyAPM/badger/v3 v3.0.0-20210809093509-ff1b2dd81165
