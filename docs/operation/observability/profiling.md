# Profiling

Banyand, the server of BanyanDB, supports profiling automatically. The profiling data is collected by the `pprof` package and can be accessed through the `/debug/pprof` endpoint. The pprof server listen address is set by the `pprof-listener-addr` flag, defaulting to `:6060`.

Refer to the [pprof documentation](https://golang.org/pkg/net/http/pprof/) for more information on how to use the profiling data.
