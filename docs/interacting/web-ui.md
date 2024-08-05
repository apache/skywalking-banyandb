# Web application

The web application is hosted at [skywalking-banyandb-webapp](http://localhost:17913/) when you boot up the BanyanDB server.
The web ui could use to query data stored in streams, measures, and properties.

## Stream
![web-ui-stream.png](https://skywalking.apache.org/doc-graph/banyandb/v0.7.0/web-ui/web-ui-stream.png)

The stream page shows the data stored in the stream. The menu on the left side shows the list of streams, index-rule, index-rule-binding and organized by their groups. 

The user can select the stream to view the data, the data is displayed in a table format and can query/filter the data by the following:
- Select the `Time Range` to be queried. 
- Select the `Tag Families` to be queried.
- Modify/Write the query config directly on the page. The config can refer to the [bydbctl query stream](./bydbctl/query/stream.md).

## Measure
![web-ui-measure.png](https://skywalking.apache.org/doc-graph/banyandb/v0.7.0/web-ui/web-ui-measure.png)

The measure page shows the data stored in the measure. The menu on the left side shows the list of measures, index-rule, index-rule-binding and organized by their groups.

The user can select the measure to view the data, the data is displayed in a table format and can query/filter the data by the following:
- Select the `Time Range` to be queried.
- Select the `Tag Families` to be queried.
- Select the `Fields` to be queried.
- Modify/Write the query config directly on the page. The config can refer to the [bydbctl query measure](./bydbctl/query/measure.md).

## Property
![web-ui-property.png](https://skywalking.apache.org/doc-graph/banyandb/v0.7.0/web-ui/web-ui-property.png)

The property page shows the data stored in the property. The menu on the left side shows the list of properties, index-rule, index-rule-binding and organized by their groups.

The user can select the group to view the property data, the data is displayed in a table format and can view/edit/delete the property by the following:
- Click the `View` to show the value in the specific property and key.
- Click the `Edit` to `add/update/delete` the tags in the specific property.
- Click the `Delete` to delete the specific property.