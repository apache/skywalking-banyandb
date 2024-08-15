# Query [Streams](../../../concept/data-model.md#streams)
The Web UI is hosted at [skywalking-banyandb-webapp](http://localhost:17913/) when you boot up the BanyanDB server.

You can query the data stored in the stream on the Web UI.

When you select the `Stream` on the top tab, the left side menu will show the list of stream groups.
Under the group, in the `Stream` category, you can view the list of streams in the group.

![web-ui-stream.png](https://skywalking.apache.org/doc-graph/banyandb/v0.7.0/web-ui/web-ui-stream.png)
You can select the specific stream to view the data, the data is displayed in a table format and can query/filter the data by the following:
- Select the `Time Range` to be queried.
- Select the `Tag Families` to be queried.
- Modify/Write the query config directly on the page. The config can refer to the [bydbctl query stream](../../bydbctl/query/stream.md).