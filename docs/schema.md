# Schema

BanyanDB tends to control resource creation instead of auto-generation. Before ingesting data into the database, users create a hierarchical structure, determining how they are grouped and laid out. The index generation rule should be applied to the pre-defined resources, indicating how the BanyanDB generates indices.

In general, BanyanDB groups resources into several kinds:

* Time series: `Measure` and `Stream`. Resources identify incoming data as time series data points.
* Index: `IndexRule` and `IndexRuleBinding`. Resources provide a `Many to Many` indexing rule bounding to other resources
* Precalculation: `TopNAggregation`. Resources analyze and aggregate data from other resources, then corresponding output results.
* Others: `Property`. Resources complement some particular scenarios.

## Group

`Group` does not provide a mechanism for isolating groups of resources within a single banyand-server but is the minimal unit to manage physical structures. Each group contains a set of options, like retention policy, shard number, etc. Several shards distribute in a group.

Every other resource should belong to a group.

[Group Registration Operations](api-reference.md#groupregistryservice)

## Measure

`Measure` consists of a sequence of data points. Each data point contains tags and fields.

`Tags` are key-value pairs. The database engine can index tag values by referring to the index rules and rule bindings, confining the query to filtering data points based on tags bound to an index rule.

A group of selected tags composite an `entity` that points out a specific time series the data point belongs. The database engine has capacities to encode and compress values in the same time series. Users should select appropriate tag combinations to optimize the data size. Another role of `entity` is the sharding key of data points, determining how to fragment data between shards.

`Fields` are also key-value pairs like tags. But the value of each field is the actual value of a single data point. The database engine would encode and compress the field's values in the same time series. The query operation is forbidden to filter data points based on a field's value.

Another option named `interval` plays a critical role in encoding. It indicates the time range between two adjacent data points in a time series and implies that all data points belonging to the same time series are distributed based on a fixed interval. A better practice for the naming measure is to append the interval literal to the tail, for example, `service_cpm_minute`.

[Measure Registration Operations](api-reference.md#measureregistryservice)

## Stream

`Stream` shares many details with `Measure` except for abandoning `field`. Stream focus on the high throughput data collection, for example, tracing and logging. The database engine also supports compressing stream entries based on `entity`, but no encode process is involved.

[Stream Registration Operations](api-reference.md#streamregistryservice)

## Property

`Property` is a standard key-value store. Users could store their metadata or items to a property and get a [sequential consistency](https://en.wikipedia.org/wiki/Consistency_model#Sequential_consistency) guarantee. BanyanDB's motivation for introducing such a particular structure is to support most APM scenarios that need to store critical data, especially for a distributed database cluster.

Users __DO NOT__ have to define a schema before writing a Property

## IndexRule and IndexRuleBinding

An `IndexRule` indicates which tags are indexed. An `IndexRuleBinding` binds an index rule to the target resources or the `subject`. There might be several rule bindings to a single resource, but their effective time range could NOT overlap.

IndexRule supports selecting two distinct kinds of index structures. `Inverted` index is the primary option when users set up an index rule. It's suitable for most tag indexing due to a better memory usage ratio and query performance. When there are many unique tag values here, such as the `ID` tag and numeric duration tag, the `Tree` index could be better. This index saves much memory space with high-cardinality data sets.

IndexRule provides a `Global` location to place some indices on a higher layer of hierarchical structure. This option intends to optimize the full-scan operation for some querying cases of no time range specification, such as finding spans from a trace by `trace_id`.

[IndexRule Registration Operations](api-reference.md#indexruleregistryservice)

[IndexRuleBinding Registration Operations](api-reference.md#indexrulebindingregistryservice)

## TopNAggregation (TBD)
