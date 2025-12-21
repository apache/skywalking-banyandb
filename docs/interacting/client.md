## Client APIs

Clients can connect to the BanyanDB server — or to a **liaison** node in a cluster deployment — via **gRPC** or **HTTP**.  
The **liaison** acts as a gateway node that routes client requests and coordinates interactions within the cluster.  
Through this connection, clients can **query** existing data or **perform bulk data writes**.

All Protocol Buffer (`.proto`) definitions used by the client are maintained in the  
[BanyanDB Client Proto Definitions Repository](https://github.com/apache/skywalking-banyandb-client-proto).

Since version **0.10.0**, BanyanDB introduces **schema-based validation** for all write operations.  
Each write request must strictly follow the schema definitions defined in the database to ensure data consistency and integrity.