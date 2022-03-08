## 服务说明

基于 raft 实现的数据存储服务, 能实现 2n+1 台节点的数据一致性存储.

储存服务, 服务有两个 servant:

- RaftObj: raft 端口接口
- StorageObj: 业务服务模块

使用者调用StorageObj 接口即可, RaftObj是提供给raft协议协商使用.

## 配置文件

服务的配置文件格式如下:
```xml
      <root>
        <raft>
            #选举超时时间(毫秒) 
            electionTimeoutMilliseconds = 3000
            #leader和从机间心跳时间(毫秒)
            heartbeatPeriodMilliseconds = 300
            #数据制作快照的时间(毫秒)
            snapshotPeriodSeconds       = 6000
            #同步数据时, 每个请求最大的日志条数
            maxLogEntriesPerRequest     = 100
            #同步数据时, 内存队列最大的数据条数
            maxLogEntriesMemQueue       = 3000
            #同部数据时, 正在传输的数据最大条数
            maxLogEntriesTransfering    = 1000
        </raft>
        #数据数据的路径
        storage-path=/storage-data
      </root>
```

## 部署说明

部署主要关注几点:
- 需要至少部署(2n+1)台节点, 至少三台节点
- 其中2n-1台节点down机后, 集群也不会影响, 仍然能正常工作
- 当在TARS框架上增加节点时, 请注意一台一台增加, 不要同时增加多台, 观察日志节点正常后才能增加下一台
- 增加新节点, 会自动同步数据, 原则上不需要做任何处理
- 减少节点也同理
- 数据目录在配置文件中指定
- 如果使用K8SFramework机制部署时, 请注意要开启LocalPV支持, 且增加mount路径(mount数据目录)

## 使用说明

- 使用c++, 底层数据存储用rockesdb, 实现mkey+ukey+data的存储方式
- mkey是住key, ukey是联合key, data是实际数据
- 底层数据采用rocksdb来存储
- 支持多张表, 使用时需要调用createTable来创建表
- 注意mkey/ukey只能使用字符串(不能用二进制数据!!!), 且在rocksdb中存储是按照字符串从小到大排序的, 因此遍历时可以按照顺序遍历
- 支持set/get/del以及对应的批量操作
- 写数据接口最终都转发到leader来执行写处理
- 查询数据接口可以指定是否在leader执行(保证刚写进去能马上查到)
- 注意查询每次指定leader, 会影响效率, 但是如果不指定leader会有数据延迟(通常都是毫秒级别) 
- 支持数据version版本一致性
- 支持基于时间戳的覆盖(只能更大的时间戳覆盖小时间错的数据)
- 支持超时自动淘汰
- 在已知mkey的情况下, 可以根据ukey来遍历数据

## 性能说明

- raft协议性能总体偏低, 因为raft协议几乎运行在一个线程上, 无法充分使用CPU
- 存储的性能, buffer较小的情况下(<1k), 3机的存储集群, 大概能支持1w/s的写请求, 如果读请求不落在leader上, 则tps能翻三倍以上(三节点的情况下).

## 未来计划支持的

- 如果实际buffer是json格式时, 支持针对某个字段的更新
- 支持集群, 即数据分裂到多机