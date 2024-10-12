# Data Lifecycle

## [Measures](../concept/data-model.md#measures) and [Streams](../concept/data-model.md#streams)

Due to the design of BanyanDB, the data in the `Measures and Streams` can not be deleted directly.
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

For more details about how they works, please refer to the [data rotation](../concept/rotation.md).

## [Property](../concept/data-model.md#properties)

`Property` data provides both [CRUD](./bydbctl/property.md) operations and TTL mechanism.

The TTL field in a property is used to set the time to live of the property. The property will be deleted automatically after the TTL.
It's a string in the format of "1h", "2m", "3s", "1500ms". It defaults to 0s, which means the property never expires.

For example, the following command will create a property with a TTL of 1 hour:

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
ttl: "1h"
EOF
```

"General-Service" will be dropped after 1 hour. If you want to extend the TTL, you could use the "keepalive" operation. The "lease_id" is returned in the apply response. 
You can use get operation to get the property with the lease_id as well.

```shell
bydbctl property get -g sw -n temp_data --id General-Service
```

```yaml
...
lease_id: 7587880824757265022
```

The lease_id is used to keep the property alive. You can use keepalive operation to keep the property alive.
When the keepalive operation is called, the property's TTL will be reset to the original value.

```shell
bydbctl property keepalive --lease_id 7587880824757265022
```

"General-Service" lives another 1 hour.

You can also manage the Property by other clients such as [Web-UI](./web-ui/property.md) or [Java-Client](java-client.md).

## The API reference 
- [Group Registration Operations](../api-reference.md#groupregistryservice)
- [ResourceOpts Definition](../api-reference.md#resourceopts)
- [PropertyService v1](../api-reference.md#propertyservice)