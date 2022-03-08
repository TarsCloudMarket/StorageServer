## Service description

The data storage service based on raft can realize the data consistency storage of 2n + 1 nodes.
The storage service has two servants:
- RaftObj: raft port interface
- StorageObj: business service module

The user can call the StorageObj interface, RaftObj is provided for raft protocol negotiation

## Configuration file

The service configuration file format is as follows:

```xml

<root>
    <raft>
        #Election timeout (MS)
        electionTimeoutMilliseconds = 3000
        #Heartbeat time between leader and slave (MS)
        heartbeatPeriodMilliseconds = 300
        #Time taken to snapshot data (MS)
        snapshotPeriodSeconds = 6000
        #Maximum number of logs per request when synchronizing data
        maxLogEntriesPerRequest = 100
        #The maximum number of data pieces in the memory queue when synchronizing data
        maxLogEntriesMemQueue = 3000
        #The maximum number of pieces of data being transmitted when the same data
        maxLogEntriesTransfering = 1000
    </raft>

    #Data path
    storage-path=/storage-data

</root>

```

## Deployment description

The deployment focuses on:
- At least (2n + 1) nodes and at least three nodes need to be deployed
- After 2N-1 nodes are down, the cluster will not be affected and can still work normally
- When adding nodes on the tars framework, please note that one node is added one by one. Do not add multiple nodes at the same time. Observe that the log nodes are normal before adding the next one
- Adding a new node will automatically synchronize the data. In principle, no processing is required
- The same goes for reducing nodes
- The data directory is specified in the configuration file
- If you deploy using k8sframework mechanism, please note that you should enable localpv support and increase the mount path (mount data directory)

## Instructions for use

- Using C++, the underlying data storage uses rockesdb to realize the storage mode of mkey + ukey + data
- Mkey is the resident key, ukey is the joint key, and data is the actual data
- The underlying data is stored in rocksdb
- Multiple tables are supported. When using, you need to call createTable to create tables
- Note that mkey/ukey can only use strings (not binary data!!!), And the storage in rocksdb is sorted according to the string from small to large, so it can be traversed in order
- Set/get/del and corresponding batch operations are supported
- The write data interface is eventually forwarded to the leader to perform write processing
- The query data interface can specify whether to execute in the leader (ensure that you can find it immediately after writing it in)
- Note that every time a leader is specified in the query, it will affect the efficiency, but if the leader is not specified, there will be data delay (usually in milliseconds)
- Support data version consistency
- Support timestamp based coverage (only larger timestamps can cover data with small time errors)
- Support timeout automatic elimination
- When mkey is known, the data can be traversed according to ukey 
- If the data format is JSON format, a field in update JSON is supported (only number/string/bool type is supported)


## Performance description

- The overall performance of raft protocol is low, because raft protocol runs on almost one thread and can not make full use of CPU
- For storage performance, when the buffer is small (< 1K), the three machine storage cluster can support 1W/s write requests. If the read requests do not fall on the leader, the tps can be more than tripled for three nodes cluster.

## Supported by future plans

- If the actual buffer is in JSON format, the update for a field is supported
- Support clustering, that is, data splitting to multiple machines