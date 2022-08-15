//
// Created by jarod on 2019-07-22.
//

#ifndef LIBRAFT_COUNTIDIMP_H
#define LIBRAFT_COUNTIDIMP_H

#include "Storage.h"
//#include "RaftNode.h"

using namespace Base;
class RaftNode;
class StorageStateMachine;

class StorageImp : public Storage
{
public:

    virtual void initialize();

    virtual void destroy();

    /**
     * 创建表
     * @param table 
     * @return 
     */
    virtual int createTable(const string &table, CurrentPtr current);

	/**
	 * 列出所有table
	 * @param queue
	 * @param current
	 * @return
	 */
	virtual int listTable(vector<string> &tables, CurrentPtr current);

    /**
     * 是否有数据
     * @param options
     * @param skey
     * @param current
     * @return
     */
    virtual int has(const Options &options, const StorageKey &skey, CurrentPtr current);

    /**
     * 读取数据
     */
    virtual int get(const Options &options, const StorageKey &skey, StorageValue &data, CurrentPtr current);

    /**
     * 写数据
     */    
    virtual int set(const StorageData &data, CurrentPtr current);

	/**
	 * 更新数据
	 */
	virtual int update(const StorageJson &data, CurrentPtr current);

    /**
     * 删除数据
     */
    virtual int del(const StorageKey &skey, CurrentPtr current);

    /**
     * 批量检查是否存在数据
     * @param options
     * @param skey
     * @param rsp
     * @param current
     * @return
     */
    virtual int hasBatch(const Options &options, const vector<StorageKey> &skey, map<StorageKey, int> &rsp, CurrentPtr current);

    /**
     * 读取数据
     */
    virtual int getBatch(const Options &options, const vector<StorageKey> &skey, vector<StorageData> &data, CurrentPtr current);

    /**
     * 更新某个字段
     */    
    virtual int setBatch(const vector<StorageData> &data, map<StorageKey, int> &rsp, CurrentPtr current);

	/**
	 * 批量更新
	 * @param data
	 * @param current
	 * @return
	 */
	virtual int updateBatch(const vector<StorageJson> &data, CurrentPtr current);

	/**
     * 删除数据
     */
    virtual int delBatch(const vector<StorageKey> &skey, CurrentPtr current);

    /**
     * 遍历数据
     */    
    virtual int trans(const Options &options, const PageReq &req, vector<StorageData> &data, CurrentPtr current);

	/**
	 * 创建队列
	 * @return int, S_OK: 成功, <0: 失败
	 */
	virtual int createQueue(const string &queue, CurrentPtr current);

	/**
	 * 列出所有queue
	 * @param queue
	 * @param current
	 * @return
	 */
	virtual int listQueue(vector<string> &queue, CurrentPtr current);

	/**
	 * 队列放入数据
	 * @return int, S_OK: 成功, <0: 失败
	 */
	virtual int push_queue(const vector<QueuePushReq> &req, CurrentPtr current);

	/**
	 * 队列从获取数据
	 * @return int, S_OK: 成功, <0: 失败 or S_NO_DATA
	 */
	virtual int get_queue(const Options &options, const QueuePopReq &req, vector<QueueRsp> &rsp, CurrentPtr current);

	/**
	 * 队列从获取数据并删除
	 * @return int, S_OK: 成功, <0: 失败 or S_NO_DATA
	 */
	virtual int pop_queue(const QueuePopReq &req, vector<QueueRsp> &rsp, CurrentPtr current);

	/**
	 * 删除数据
	 * @param queue
	 * @param index
	 * @param current
	 * @return
	 */
	virtual int deleteQueueData(const vector<QueueIndex> &req, CurrentPtr current);

	/**
	 * 是否拥有数据
	 * @param queue
	 * @param index
	 * @param has
	 * @param current
	 * @return
	 */
	virtual int getQueueData(const Options &options, const vector<QueueIndex> &req, vector<QueueRsp> &rsp, CurrentPtr current);

	/**
	 * 批量处理写
	 * @param data
	 * @return
	 */
	virtual int doBatch(const BatchDataReq &req, BatchDataRsp &rsp, CurrentPtr current);

protected:

	shared_ptr<RaftNode>    _raftNode;

	shared_ptr<StorageStateMachine> _stateMachine;
};


#endif //LIBRAFT_RAFTCLIENTIMP_H
