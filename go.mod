module github.com/apache/skywalking-banyandb

go 1.16

require (
	github.com/dgraph-io/badger/v3 v3.2011.1
	github.com/emicklei/dot v0.16.0
	github.com/golang/mock v1.4.4
	github.com/google/flatbuffers v2.0.0+incompatible
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/terraform v0.15.3
	github.com/oklog/run v1.1.0
	github.com/onsi/ginkgo v1.16.4
	github.com/onsi/gomega v1.13.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.7.0
	go.uber.org/g v1.6.0
	go.uber.org/zap v1.16.0
	google.golang.org/grpc v1.37.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/dgraph-io/badger/v3 v3.2011.1 => github.com/SkyAPM/badger/v3 v3.0.0-20210527215642-f1c960d2de88
