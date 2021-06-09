module github.com/apache/skywalking-banyandb

go 1.16

require (
	github.com/dgraph-io/badger/v3 v3.2011.1
	github.com/golang/mock v1.3.1
	github.com/google/flatbuffers v2.0.0+incompatible
	github.com/oklog/run v1.1.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/objx v0.3.0 // indirect
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.7.0
	go.uber.org/multierr v1.6.0
	go.uber.org/zap v1.16.0
	google.golang.org/grpc v1.37.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/dgraph-io/badger/v3 v3.2011.1 => github.com/SkyAPM/badger/v3 v3.0.0-20210527215642-f1c960d2de88
