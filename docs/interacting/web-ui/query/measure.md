# Query [Measures](../../../concept/data-model.md#measures) on the Web UI
The Web UI is hosted at [skywalking-banyandb-webapp](http://localhost:17913/) when you boot up the BanyanDB server.

You can query the data stored in the measure on the Web UI.

When you select the `Measure` on the top tab, the left side menu will show the list of measure groups. 
Under the group, in the `Measure` category, you can view the list of measure in the group.

![web-ui-measure.png](https://skywalking.apache.org/doc-graph/banyandb/v0.7.0/web-ui/web-ui-measure.png)

The measure page shows the data stored in the measure. The menu on the left side shows the list of measures, index-rule, index-rule-binding and organized by their groups.

You can select the specific measure to view the data, the data is displayed in a table format and can query/filter the data by the following:
- Select the `Time Range` to be queried.
- Select the `Tag Families` to be queried.
- Select the `Fields` to be queried.
- Modify/Write the query config directly on the page. The config can refer to the [bydbctl query measure](../../bydbctl/query/measure.md).