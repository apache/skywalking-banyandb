# CRUD Property

CRUD operations create/update, read and delete property.

Property stores the user defined data.

[`bydbctl`](../../clients.md#command-line) is the command line tool in examples.

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
    name: ui_template
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
    name: ui_template
  id: General-Service
tags:
- key: state
  value:
    str:
      value: "failed"
EOF
```

## Get operation

Get operation gets a property.

### Examples of getting

```shell
$ bydbctl property get -g sw -n ui_template --id General-Service
```

The operation could filter data by tags.

```shell
$ bydbctl property get -g sw -n ui_template --id General-Service --tags state
```

## Delete operation

Delete operation delete a property.

### Examples of deleting

```shell
$ bydbctl property delete -g sw -n ui_template --id General-Service.
```

The delete operation could remove specific tags instead of the whole property.

```shell
$ bydbctl property delete -g sw -n ui_template --id General-Service --tags state.
```

## List operation

List operation lists all properties.

### Examples of listing

```shell
$ bydbctl property list -g sw -n ui_template
```

## API Reference

[MeasureService v1](../../api-reference.md#PropertyService)
