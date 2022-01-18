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
     * 写数据
     */    
    virtual int setBatch(const vector<StorageData> &data, map<StorageKey, int> &rsp, CurrentPtr current);

    /**
     * 删除数据
     */
    virtual int delBatch(const vector<StorageKey> &skey, CurrentPtr current);

    /**
     * 遍历数据
     */    
    virtual int trans(const Options &options, const PageReq &req, vector<StorageData> &data, CurrentPtr current);

protected:

	shared_ptr<RaftNode>    _raftNode;

	shared_ptr<StorageStateMachine> _stateMachine;
};


#endif //LIBRAFT_RAFTCLIENTIMP_H