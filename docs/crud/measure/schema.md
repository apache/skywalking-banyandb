# CRUD Measures

CRUD operations create, read, update and delete measures.

## Create operation

Create operation adds a new measure to the database's metadata registry repository. If the measure does not currently exist, create operation will create the schema.

### Examples

`bydbctl` is the command line tool to create a measure in this example.

A measure belongs to a unique group. We should create such a group with a catalog `CATALOG_MEASURE`
before creating a measure.

```shell
$ bydbctl group create -f - <<EOF
metadata:
  name: sw_metric
catalog: CATALOG_MEASURE
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

The group creates two shards to store measure data points. Every one day, it would create a
segment which will generate a block every 2 hours.

The data in this group will keep 7 days.

Then, below command will create a new measure:

```shell
$ bydbctl measure create -f - <<EOF
metadata:
  name: service_cpm_minute
  group: sw_metric
tag_families:
- name: default
  tags:
  - name: id
    type: TAG_TYPE_ID
  - name: entity_id
    type: TAG_TYPE_STRING
fields:
- name: total
  field_type: FIELD_TYPE_INT
  encoding_method: ENCODING_METHOD_GORILLA
  compression_method: COMPRESSION_METHOD_ZSTD
- name: value
  field_type: FIELD_TYPE_INT
  encoding_method: ENCODING_METHOD_GORILLA
  compression_method: COMPRESSION_METHOD_ZSTD
entity:
  tag_names:
  - entity_id
interval: 1m
EOF
```

`service_cpm_minute` expect to ingest a series of data points with one minute interval.

## Read operation

## Update operation

## Delete operation

## API Reference

[MeasureService v1](../../api-reference.md#measureservice)
