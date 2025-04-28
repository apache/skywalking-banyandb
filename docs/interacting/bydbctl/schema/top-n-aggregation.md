# CRUD [TopNAggregation](../../../concept/data-model.md#topnaggregation)

CRUD operations create, read, update and delete top-n-aggregations.

[bydbctl](../bydbctl.md) is the command line tool in examples.

Find the Top-N entities from a dataset in a time range is a common scenario. Top-n-aggregation aims to pre-calculate the top/bottom entities during the measure writing phase.

## Create operation

Create operation adds a new top-n-aggregation to the database's metadata registry repository. If the top-n-aggregation does not currently exist, create operation will create the schema.

### Examples of creating

A top-n-aggregation belongs to a unique group. We should create such a group with a catalog `CATALOG_MEASURE` before creating a top-n-aggregation.

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

The top-n-aggregation monitors the data ingestion of the source measure while  generating ranked top-N entities. We should create such a measure before creating a top-n-aggregation.

```shell
bydbctl measure create -f - <<EOF
metadata:
  name: service_instance_cpm_minute
  group: sw_metric
tag_families:
  - name: default
    tags:
      - name: id
        type: TAG_TYPE_STRING
      - name: entity_id
        type: TAG_TYPE_STRING
      - name: service_id
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
  tagNames: 
    - service_id
    - entity_id
sharding_key:
  tagNames: 
    - service_id
interval: 1m
EOF
```

`service_instance_cpm_minute` expects to ingest per-minute metrics for individual service instances. 

Then, the below command will create a new top-n-aggregation:

```shell
bydbctl topn create -f - <<EOF
metadata:
  name: service_instance_cpm_minute_top_bottom_100
  group: sw_metric
source_measure:
  name: service_instance_cpm_minute
  group: sw_metric
field_name: value
field_value_sort: SORT_UNSPECIFIED
group_by_tag_names:
  - service_id
counters_number: 1000
lru_size: 10
EOF
```
`service_instance_cpm_minute_top_bottom_100` is watching the data ingesting of the source measure `service_instance_cpm_minute` to generate both top 1000 and bottom 1000 entity cardinalities. If only Top 1000 or Bottom 1000 is needed, the `field_value_sort` could be `DESC` or `ASC` respectively.

- `SORT_DESC`: Top-N. In a series of `1,2,3...1000`. Top10’s result is `1000,999...991`.
- `SORT_ASC`: Bottom-N. In a series of `1,2,3...1000`. Bottom10’s result is `1,2...10`.

Tags in `group_by_tag_names` are used as dimensions. These tags can be searched (only equality is supported) in the query phase. Tags do not exist in `group_by_tag_names` will be dropped in the pre-calculating phase.

`counters_number` denotes the number of entity cardinality. As the above example shows, calculating the Top 100 among 10 thousands is easier than among 10 millions.

`lru_size` is a late data optimizing flag. The higher the number, the more late data, but the more memory space is consumed.

More top-n-aggregation information can be found in [here](../../../concept/data-model.md#topnaggregation).

## Get operation

Get(Read) operation gets a top-n-aggregation's schema.

### Examples of getting

```shell
bydbctl topn get -g sw_metric -n service_instance_cpm_minute_top_bottom_100
```

## Update operation

Update operation changes a top-n-aggregation's schema.

### Examples of updating

```shell
bydbctl topn update -f - <<EOF
metadata:
  name: service_instance_cpm_minute_top_bottom_100
  group: sw_metric
source_measure:
  name: service_instance_cpm_minute
  group: sw_metric
field_name: value
field_value_sort: SORT_UNSPECIFIED
group_by_tag_names:
  - service_id
  - entity_id
counters_number: 1000
lru_size: 10
EOF
```

## Delete operation

Delete operation removes a top-n-aggregation's schema.

### Examples of deleting

```shell
bydbctl topn delete -g sw_metric -n service_instance_cpm_minute_top_bottom_100
```

## List operation

The list operation shows all top-n-aggregation' schema in a group.

### Examples of listing

```shell
bydbctl topn list -g sw_metric
```

## API Reference

[TopNAggregation Registration Operations](../../../api-reference.md#topnaggregationregistryservice)
