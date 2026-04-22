## Client APIs

Clients can connect to the BanyanDB server — or to a **liaison** node in a cluster deployment — via **gRPC** or **HTTP**.  
The **liaison** acts as a gateway node that routes client requests and coordinates interactions within the cluster.  
Through this connection, clients can **query** existing data or **perform bulk data writes**.

All Protocol Buffer (`.proto`) definitions used by the client are maintained in the  
[BanyanDB Client Proto Definitions Repository](https://github.com/apache/skywalking-banyandb-client-proto).

Since version **0.10.0**, BanyanDB introduces **schema-based validation** for all write operations.  
Each write request must strictly follow the schema definitions defined in the database to ensure data consistency and integrity.

## Stream element identity

For stream writes, `element_id` is optional.

- If the client sets `element_id`, BanyanDB derives the internal element identity from the group, stream name, and `element_id` value. Use a stable `element_id` when retries or re-sends should refer to the same logical element.
- If the client omits `element_id`, BanyanDB generates a server-side element ID for that write. This is convenient for append-only ingestion, but repeated writes without a stable `element_id` are treated as different elements.

See [Query Streams](./bydbctl/query/stream.md#element-identity-and-deduplication) for how element identity affects query results.
