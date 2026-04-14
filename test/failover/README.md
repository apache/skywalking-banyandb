# Failover and Resilience Test

## Setup the Cluster

See [Setup the Cluster](setup.md).

## Case 1: Liaison Node Failure

### Steps to simulate a liaison node failure

1. Add an annotation "failover-try=1" to the Liaison pod to simulate a failure.
2. A new Liaison pod will be created, and the old Liaison pod will be in the `Terminating` state.
3. Check the status of the Liaison pods and OAP console.
4. Check write and query operations.

### Result of the liaison node failure

- The first Liaison pod is in the `Terminating` state.
- The second Liaison pod is in the `Running` state.
- The cluster is still available.
- The trace and metrics(5 services) write and read operations are still available.

## Case 2: Data Node Failure

### Steps to simulate a data node failure

1. Scale the Data pod to 3 replicas. They are `banyandb-0`, `banyandb-1`, and `banyandb-2`.
2. Scale the Data pod to 2 replica. `banyandb-2` pod will be terminated.
3. Check the status of the Data pods, OAP console, and Liaison console.
4. Check write and query operations.

### Result of the data node failure

- The `banyandb-1` pod is in the `Terminating` state.
- The cluster is still available.
- OAP might face "fail to execute the query plan for measure events_minute: broadcast errors: failed to publish message to 10. │
│ 244.0.76:17912: failed to get stream for node 10.244.0.76:17912: rpc error: code = Canceled desc = grpc: the client connection is closing: invalid query message" error.
- The trace and metrics(5 services) write and read operations are still available.
- Partial data loss might occur as the `banyandb-2` is down.

```yaml
2024-08-15 0609:
  value: 0
  isemptyvalue: true
2024-08-15 0610:
  value: 0
  isemptyvalue: true
2024-08-15 0611:
  value: 0
  isemptyvalue: true
2024-08-15 0612:
  value: 0
  isemptyvalue: true
2024-08-15 0613:
  value: 549
  isemptyvalue: false
2024-08-15 0614:
  value: 541
  isemptyvalue: false
2024-08-15 0615:
  value: 566
  isemptyvalue: false
2024-08-15 0616:
  value: 546
  isemptyvalue: false
```

