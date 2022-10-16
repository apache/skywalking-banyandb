# CRUD Property

CRUD operations create, read, update and delete property.

Property stores the user defined data.
## Create / Update operation

Apply creates a property if it's absent, or update an existed one based on a strategy.If the property does not currently exist, create operation will create the schema.

### Examples

`bydbctl` is the command line tool to create a property in this example.

A property belongs to a unique group. We should create such a group with a catalog `CATALOG_STREAM`
before creating a property.

```shell
$ bydbctl group create -f - <<EOF
metadata:
  name: default
catalog: CATALOG_STREAM
resource_opts:
  shard_num: 2
  block_interval:
    unit: UNIT_HOUR
    num: 2
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 7
EOF
```
Then, below command will create a new property:

```shell
$ bydbctl property create -f - <<EOF
metadata:
 container:
  group: default
  name: container1
 id: property1
EOF
```


## Read(Get) operation
Read operation read a property's schema.

### Examples
`bydbctl` is the command line tool to get a property in this example.
```shell
$ bydbctl property get -g default -n property1
```
## Update operation
Update operation update a property's schema.

### Examples
`bydbctl` is the command line tool to update a property in this example.
```shell
$ bydbctl property update -f - <<EOF
metadata:
 container:
  group: default
  name: container1
 id: property1
EOF
```

## Delete operation
Delete operation delete a property's schema.

### Examples
`bydbctl` is the command line tool to delete a property in this example.
```shell
$ bydbctl property delete -g default -n container1 -i property1
```

## List operation
List operation lists all properties.
### Examples
`bydbctl` is the command line tool to list all the properties in this example.
```shell
$ bydbctl property list -g default -n container1 -i property1
```