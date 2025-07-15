# Property Background Repair Observability

Based on the [Property Background Repair Strategy documentation](../concept/property-repair.md), 
this article explains how to visualize and monitor each synchronization cycle to enhance observability and debugging.

In the current UI, a Dashboard is available that presents various types of data, including **Property**-related information.
Within the Property section, you can navigate to the **Repair Selection panel** to view historical repair records, 
allowing you to track past synchronization events and inspect their outcomes in detail.

This feature is enabled by default. You can configure whether to record and display data details through the `--property-repair-obs-enabled` option.

## Data Selection

By default, the system automatically retains all background repair records for three days. 
This retention period can be configured via the `--property-repair-history-days` option.

When querying, you can use the record list to select a specific date and view the corresponding repair activity for that day.

## Tracing

After selecting a date, you can view the trace data on the left side of the UI. This data is presented in a trace view, showing:
* Which nodes were involved in the synchronization?
* Where do errors occur during the repairing process?

The trace view includes the following key information:
1. **Node Name**: The identity of the node currently executing the repair task.
2. **Target Node**: The peer node with which the current node is synchronizing Property data.
3. **Time**: The start and end timestamps of the synchronization operation.
4. **Sync Status**: A detailed list of each request and action during the synchronization process, including success and failure states.
5. **Data Updates**: If any Property data was updated during the repair, these changes are also recorded and displayed.

## Metrics

On the right side of the UI, you can view metrics related to the repair task execution. 
A dropdown menu allows you to select a specific node to inspect its detailed metrics.

The metrics panel includes the following information:
1. **Sync Request Count**: The total number of synchronization operations initiated by the selected node.
2. **Update Count**: The number of Property entries that were successfully updated, as well as those that failed to update during the repair process.
