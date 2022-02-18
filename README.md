# 介绍

## 服务说明

基于 raft 实现的数据存储服务, 能实现 2n+1 台节点的数据一致性存储.

底层 raft 库是基于: https://github.com/TarsCloudMarket/libraft

储存服务, 服务有两个 servant:

- raftObj: raft 端口接口
- StorageObj: 业务服务模块

使用者调用 raft.StorageServer.StorageOb 接口即可.

所有读接口, 都带有 leader 参数, 表示是否一定要从 leader 读取数据, 对于实时性以及数据准确性要求不那么高的请求, 可以设置 leader 为 false, 这样能提高整体集群的通信效率.

## 配置文件

## 部署说明

## 使用说明
