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

## Case 3: etcd Node Failure

### Steps to simulate an etcd node failure

1. Scale the etcd pod to 0 replicas.
2. Check the status of the OAP, Data and Liaison console.
3. Check write and query operations.

## Result of the etcd node failure

1. Liaison and Data pods are available, but will raise an error.

```json
{"level":"warn","ts":1723709128.2490797,"caller":"v3@v3.5.13/retry_interceptor.go:62","msg":"retrying of unary invoker failed","target":"etcd-endpoints://0xc00049e1e0/failover-test-etcd-0.failover-test-etcd-headless.default:2379","attempt":0,"error":"rpc error: code = DeadlineExceeded desc = latest balancer error: last connection error: connection error: desc = \"transport: Error while dialing: dial tcp 10.96.126.15:2379: connect: connection refused\""}
{"level":"error","module":"ETCD","error":"context deadline exceeded","time":"2024-08-15T08:05:28Z","message":"failed to revoke lease 8287064579165108153"}                                                                                                                            
{"level":"warn","ts":1723709216.6529357,"caller":"v3@v3.5.13/retry_interceptor.go:62","msg":"retrying of unary invoker failed","target":"etcd-endpoints://0xc00049e1e0/failover-test-etcd-0.failover-test-etcd-headless.default:2379","attempt":0,"error":"rpc error: code = DeadlineExceeded desc = latest balancer error: last connection error: connection error: desc = \"transport: Error while dialing: dial tcp: lookup failover-test-etcd-0.failover-test-etcd-headless.default.svc.cluster.local on 10.96.0.10:53: no such host\""}
{"level":"info","ts":1723709216.653035,"caller":"v3@v3.5.13/client.go:210","msg":"Auto sync endpoints failed.","error":"context deadline exceeded"} 
```

2. The trace and metrics(5 services) write and read operations are still available.
3. `swctl menu get`

## Case 4: etcd Node recovery

### Steps to recover the etcd node.

1. Scale the etcd pod to 1 replica.
2. Check the status of the OAP, Data and Liaison console.
3. Check write and query operations.

## Result of the etcd node recovery with the correct data

1. Liaison and Data pods are available, and their consoles will show:

```json
 {"level":"warn","ts":1723710245.1049383,"caller":"v3@v3.5.13/retry_interceptor.go:62","msg":"retrying of unary invoker failed","target":"etcd-endpoints://0xc00049e1e0/failover-test-etcd-0.failover-test-etcd-headless.default:2379","attempt":0,"error":"rpc error: code = Unauthenticated desc = etcdserver: invalid auth token"} 
```

The message means that the client's token is invalid. The client should re-authenticate with the correct token and reconnect.

2. The trace and metrics(5 services) write and read operations are still available.
3. `swctl menu get` will return data as expected.
4. Add a new Data node, the liaison will automatically add the new Data node to the route table.
