# Delete Data in [Measures](../concept/data-model.md#measures) and [Streams](../concept/data-model.md#streams)

Due to the design of BanyanDB, the `Property` data provides [CRUD](./bydbctl/property.md) operations,
but the data in the `Measures and Streams` can not be deleted directly.
The data will be deleted automatically based on the [Groups](../concept/data-model.md#groups) `TTL` setting.

The TTL means the `time to live` of the data in the group. 
Each group has an internal trigger which is triggered by writing events. If there is no further data, the expired data can’t get removed.

For example, the following command will create a group with a TTL of 7 days:
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
The data in this group will keep 7 days.

If you want to change the `TTL` of the data in this group to be 1 day, use the command:

```shell
bydbctl group update -f - <<EOF
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
    num: 1
EOF
```

More ttl units can be found in the [IntervalRule.Unit](../api-reference.md#intervalruleunit).

You can also manage the Group by other clients such as [Web-UI](./web-ui/schema/group.md) or [Java-Client](java-client.md).

## The API reference 
- [Group Registration Operations](../api-reference.md#groupregistryservice)
- [ResourceOpts Definition](../api-reference.md#resourceopts)