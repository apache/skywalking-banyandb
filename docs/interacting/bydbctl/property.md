# CRUD [Property](../../concept/data-model.md#properties)

CRUD operations create/update, read and delete property.

Property stores the user defined data.

[bydbctl](bydbctl.md) is the command line tool in examples.

## Apply (Create/Update) operation

Apply creates a property if it's absent, or updates an existed one based on a strategy. If the property does not currently exist, create operation will create the property.

### Examples of applying

A property belongs to a unique group. We should create such a group before creating a property.

The group's catalog should be empty.

```shell
bydbctl group create -f - <<EOF
metadata:
  name: sw
catalog: CATALOG_PROPERTY
resource_opts:
  shard_num: 2
EOF
```

Then, below command will create a new property:

```shell
bydbctl property apply -f - <<EOF
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
bydbctl property apply -f - <<EOF
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

## Delete operation

Delete operation delete a property.

### Examples of deleting

```shell
bydbctl property delete -g sw -n temp_data --id General-Service
```

## Query operation

Query operation properties in a group.

### Examples of listing in a group

```shell
bydbctl property query -f - <<EOF
groups: ["sw"]
EOF
```

Query operation queries all properties in a group with a container name.

### Examples of listing in a group with a container name

```shell
bydbctl property query -f - <<EOF
groups: ["sw"]
container: temp_data
EOF
```

Query properties with a specific tag.

### Examples of listing with a tag

```shell
bydbctl property query -f - <<EOF
groups: ["sw"]
criteria:
  condition:
    name: "state"
    op: "BINARY_OP_EQ"
    value:
      str:
        value: "succeed"
```

You can limit the number of properties to be returned.

```shell
bydbctl property query -f - <<EOF
groups: ["sw"]
limit: 1
```

You also can return partial tags of properties(tags' projection).

```shell
bydbctl property query -f - <<EOF
groups: ["sw"]
tag_projection: ["name"]
```

## API Reference

[PropertyService v1](../../api-reference.md#propertyservice)
