# bydbctl
`bydbctl` is the command line tool for interacting with BanyanDB. It is a powerful tool that can be used to create, update, read, and delete schemas. It can also be used to query data stored in streams, measures, and properties.

These are several ways to install:

* Get binaries from [download](https://skywalking.apache.org/downloads/).
* Build from [sources](https://github.com/apache/skywalking-banyandb/tree/main/bydbctl) to get latest features.

The config file named `.bydbctl.yaml` will be created in `$HOME` folder after the first CRUD command is applied.
```shell
> more ~/.bydbctl.yaml
addr: http://127.0.0.1:17913
group: ""
```

`bydbctl` leverages HTTP endpoints to retrieve data instead of gRPC.

## HTTP client

Users could select any HTTP client to access the HTTP based endpoints. The default address is `localhost:17913/api`