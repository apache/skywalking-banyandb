module github.com/apache/skywalking-banyandb

go 1.16

require (
	github.com/RoaringBitmap/roaring v0.9.1
	github.com/cespare/xxhash v1.1.0
	github.com/dgraph-io/badger/v3 v3.2011.1
	github.com/gogo/protobuf v1.3.2
	github.com/golang/mock v1.3.1
	github.com/golang/protobuf v1.5.2
	github.com/google/flatbuffers v2.0.0+incompatible
	github.com/google/go-cmp v0.5.6
	github.com/oklog/run v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/rs/zerolog v1.23.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.7.0
	go.uber.org/multierr v1.6.0
	google.golang.org/grpc v1.37.0
)

replace github.com/dgraph-io/badger/v3 v3.2011.1 => github.com/SkyAPM/badger/v3 v3.0.0-20210624023741-bd2dcfcaaa74
