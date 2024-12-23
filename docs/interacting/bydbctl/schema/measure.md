# CRUD [Measures](../../../concept/data-model.md#measures)

CRUD operations create, read, update and delete measures.

[bydbctl](../bydbctl.md) is the command line tool in examples.

## Create operation

Create operation adds a new measure to the database's metadata registry repository. If the measure does not currently exist, create operation will create the schema.

### Examples of creating

A measure belongs to a unique group. We should create such a group with a catalog `CATALOG_MEASURE`
before creating a measure.

```shell
bydbctl group create -f - <<EOF
metadata:
  name: sw_metric
catalog: CATALOG_MEASURE
resource_opts:
  shard_num: 2
  segment_interval:
    unit: UNIT_DAY
    num: 1
  ttl:
    unit: UNIT_DAY
    num: 7
EOF
```

The group creates two shards to store data points. Every day, it would create a
segment that will generate a block every 2 hours.

The data in this group will keep 7 days.

Then, the below command will create a new measure:

```shell
bydbctl measure create -f - <<EOF
metadata:
  name: service_cpm_minute
  group: sw_metric
tag_families:
- name: default
  tags:
  - name: id
    type: TAG_TYPE_STRING
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

`service_cpm_minute` expects to ingest a series of data points with a minute interval.

## Get operation

Get(Read) operation gets a measure's schema.

### Examples of getting

```shell
bydbctl measure get -g sw_metric -n service_cpm_minute
```

## Update operation

Update operation changes a measure's schema.

### Examples of updating

```shell
bydbctl measure update -f - <<EOF
metadata:
  name: service_cpm_minute
  group: sw_metric
tag_families:
- name: default
  tags:
  - name: id
    type: TAG_TYPE_STRING
  - name: entity_id
    type: TAG_TYPE_STRING
  - name: new_tag
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

You only can append new tags or fields to a measure. You can't change the existing tags or fields. The order of tags and fields is immutable, you can't change the order of tags or fields. You can't insert a new tag or field in the middle or front of the existing tags or fields.

## Delete operation

Delete operation removes a measure's schema.

### Examples of deleting

```shell
bydbctl measure delete -g sw_metric -n service_cpm_minute
```

## List operation

The list operation shows all measures' schema in a group.

### Examples of listing

```shell
bydbctl measure list -g sw_metric
```

## API Reference

[Measure Registration Operations](../../../api-reference.md#measureregistryservice)
