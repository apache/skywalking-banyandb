# CRUD Property

CRUD operations create/update, read and delete property.

Property stores the user defined data.

[`bydbctl`](../clients.md#command-line) is the command line tool in examples.

## Apply (Create/Update) operation

Apply creates a property if it's absent, or updates an existed one based on a strategy. If the property does not currently exist, create operation will create the property.

### Examples of applying

A property belongs to a unique group. We should create such a group before creating a property.

The group's catalog should be empty.

```shell
$ bydbctl group create -f - <<EOF
metadata:
  name: sw
EOF
```

Then, below command will create a new property:

```shell
$ bydbctl property apply -f - <<EOF
metadata:
  container:
    group: sw
    name: temp_data
  id: General-Service
tags:
- key: name
  value:
    str:
      value: "hello"
- key: state
  value:
    str:
      value: "succeed"
EOF
```

The operation supports updating partial tags.

```shell
$ bydbctl property apply -f - <<EOF
metadata:
  container:
    group: sw
    name: temp_data
  id: General-Service
tags:
- key: state
  value:
    str:
      value: "failed"
EOF
```

TTL is supported in the operation.

```shell
$ bydbctl property apply -f - <<EOF
metadata:
  container:
    group: sw
    name: temp_data
  id: General-Service
tags:
- key: state
  value:
    str:
      value: "failed"
ttl: "1h"
```

## Get operation

Get operation gets a property.

### Examples of getting

```shell
$ bydbctl property get -g sw -n temp_data --id General-Service
```

The operation could filter data by tags.

```shell
$ bydbctl property get -g sw -n temp_data --id General-Service --tags state
```

## Delete operation

Delete operation delete a property.

### Examples of deleting

```shell
$ bydbctl property delete -g sw -n temp_data --id General-Service
```

The delete operation could remove specific tags instead of the whole property.

```shell
$ bydbctl property delete -g sw -n temp_data --id General-Service --tags state
```

## List operation

List operation lists all properties in a group.

### Examples of listing in a group

```shell
$ bydbctl property list -g sw
```

List operation lists all properties in a group with a name.

### Examples of listing in a group with a name

```shell
$ bydbctl property list -g sw -n temp_data
```

## TTL field in a property

TTL field in a property is used to set the time to live of the property. The property will be deleted automatically after the TTL.

This functionality is supported by the lease mechanism. The readonly lease_id field is used to identify the lease of the property.

### Examples of setting TTL

```shell
$ bydbctl property apply -f - <<EOF
metadata:
  container:
    group: sw
    name: temp_data
  id: General-Service
tags:
- key: state
  value:
    str:
      value: "failed"
ttl: "1h"
EOF
```

The lease_id is returned in the response. 
You can use get operation to get the property with the lease_id as well.

```shell
$ bydbctl property get -g sw -n temp_data --id General-Service
```

The lease_id is used to keep the property alive. You can use keepalive operation to keep the property alive.
When the keepalive operation is called, the property's TTL will be reset to the original value.

```shell
$ bydbctl property keepalive --lease_id 1
```


## API Reference

[MeasureService v1](../api-reference.md#PropertyService)
