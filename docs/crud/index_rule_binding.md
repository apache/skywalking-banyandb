# CRUD indexRuleBindings

CRUD operations create, read, update and delete index rule bindings.

An index rule binding is a bridge to connect several index rules to a subject.
This binding is valid between `begin_at_nanoseconds` and `expire_at_nanoseconds`, that provides flexible strategies to control how to generate time series indices.

[`bydbctl`](../clients.md#command-line) is the command line tool in examples.

## Create operation

Create operation adds a new index rule binding to the database's metadata registry repository. If the index rule binding does not currently exist, create operation will create the schema.

### Examples

An index rule binding belongs to a unique group. We should create such a group with a catalog `CATALOG_STREAM` before creating a index rule binding. The subject(stream/measure) and index rule MUST live in the same group with the binding.

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

The group creates two shards to store indexRuleBinding data points. Every one day, it would create a
segment which will generate a block every 2 hours.

The data in this group will keep 7 days.

Then, below command will create a new indexRuleBinding:

```shell
$ bydbctl indexRuleBinding create -f - <<EOF
metadata:
  name: stream_binding
  group: sw_stream
rules:
- trace_id
- duration
- endpoint_id
- status_code
- http.method
- db.instance
- db.type
- mq.broker
- mq.queue
- mq.topic
- extended_tags
subject:
  catalog: CATALOG_STREAM
  name: sw
begin_at: '2021-04-15T01:30:15.01Z'
expire_at: '2121-04-15T01:30:15.01Z'
EOF
```

The YAML contains:

* `rules`: references to the name of index rules.
* `subject`: stream or measure's name and catalog.
* `begin_at` and `expire_at`: the TTL of this binding.

## Get operation

Get(Read) operation gets an index rule binding's schema.

### Examples of getting

```shell
$ bydbctl indexRuleBinding get -g sw_stream -n stream_binding
```

## Update operation

Update operation update an index rule binding's schema.

### Examples updating

```shell
$ bydbctl indexRuleBinding update -f - <<EOF
metadata:
  name: stream_binding
  group: sw_stream
rules:
- trace_id
- duration
- endpoint_id
- status_code
- http.method
- db.instance
- db.type
- mq.broker
- mq.queue
- mq.topic
# Remove this rule
# - extended_tags
subject:
  catalog: CATALOG_STREAM
  name: sw
begin_at: '2021-04-15T01:30:15.01Z'
expire_at: '2121-04-15T01:30:15.01Z'
EOF
```

The new YAML removed the index rule `extended_tags`'s binding.

## Delete operation

Delete operation delete an index rule binding's schema.
### Examples of deleting

```shell
$ bydbctl indexRuleBinding delete -g sw_stream -n stream_binding
```

## List operation

List operation list all index rule bindings in a group.

### Examples of listing

```shell
$ bydbctl indexRuleBinding list -g sw_stream
```

## API Reference

[indexRuleBindingService v1](../api-reference.md#IndexRuleBindingRegistryService)