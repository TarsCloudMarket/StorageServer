//
// Created by jarod on 2019-06-06.
//

#ifndef LIBRAFT_STORAGESTATEMACHINE_H
#define LIBRAFT_STORAGESTATEMACHINE_H

#include <mutex>
#include "util/tc_thread_rwlock.h"
#include "util/tc_thread.h"
#include "Storage.h"
#include "StateMachine.h"

namespace rocksdb
{
class DB;
class Iterator;
class Comparator;
class ColumnFamilyHandle;
class WriteBatch;
}

using namespace Base;

class ApplyContext;

class StorageStateMachine : public StateMachine
{
public:
	const static string SET_TYPE  ;
	const static string BSET_TYPE ;
	const static string DEL_TYPE  ;
	const static string BDEL_TYPE ;
	const static string TABLE_TYPE;
	const static string SET_JSON_TYPE  ;
	const static string BSET_JSON_TYPE;

	const static string QUEUE_TYPE;
	const static string PUSH_BACK_TYPE;
	const static string PUSH_FRONT_TYPE;
	const static string POP_BACK_DEL_TYPE;
	const static string POP_FRONT_DEL_TYPE;
	const static string DEL_DATA_TYPE;
	const static string CLEAR_QUEUE_TYPE;

	/**
	 * 构造
	 * @param dataPath
	 */
	StorageStateMachine(const string &dataPath);

	/**
	 * 析构
	 */
	virtual ~StorageStateMachine();

	/**
     * 对状态机中数据进行snapshot，每个节点本地定时调用
     * @param snapshotDir snapshot数据输出目录
     */
	virtual void onSaveSnapshot(const string &snapshotDir);

	/**
	 * 读取snapshot到状态机，节点启动时 或者 节点安装快照后 调用
	 * @param snapshotDir snapshot数据目录
	 */
	virtual bool onLoadSnapshot(const string &snapshotDir);

	/**
	 * 启动时加载数据
	 * @return
	 */
	virtual int64_t onLoadData();

	/**
     * 将数据应用到状态机
     * @param dataBytes 数据二进制
     * @param appliedIndex, appliedIndex
     * @param callback, 如果是Leader, 且网路请求过来的, 则callback有值, 否则为NULL
     */
	virtual void onApply(const char *buff, size_t length, int64_t appliedIndex, const shared_ptr<ApplyContext> &context);

	/**
	 * 变成Leader
	 * @param term
	 */
	virtual void onBecomeLeader(int64_t term);

	/**
	 * 变成Follower
	 */
	virtual void onBecomeFollower();

	/**
	 * 开始选举的回调
	 * @param term 选举轮数
	 */
	virtual void onStartElection(int64_t term);

	/**
	 * 节点加入集群(Leader or Follower) & LeaderId 已经设置好!
	 * 此时能够正常对外提供服务了, 对于Follower收到请求也可以转发给Leader了
	 */
	virtual void onJoinCluster();
	/**
	 * 节点离开集群(重新发起投票, LeaderId不存在了)
	 * 此时无法正常对外提供服务了, 请求不能发送到当前节点
	 */
	virtual void onLeaveCluster();

	/**
	* 开始从Leader同步快照文件
	*/
	virtual void onBeginSyncShapshot();

	/**
	 * 结束同步快照
	 */
	virtual void onEndSyncShapshot();

	/**
	 * 获取单条数据
	 * @param skey
	 * @return
	 */
	int has(const StorageKey &skey);

	/**
	 * 获取单条数据
	 * @param skey
	 * @param data
	 * @return
	 */
	int get(const StorageKey &skey, StorageValue &data);

	/**
	 * 批量获取数据
	 * @param skey
	 * @param data
	 * @return
	 */
	int get(const vector<StorageKey> &skey, vector<StorageData> &data);

	/**
	 * 批量检查
	 * @param skey
	 * @param rsp
	 * @return
	 */
	int hasBatch(const vector<StorageKey> &skey, map<StorageKey, int> &rsp);

	/**
	 * 遍历数据
	 * @param req
	 * @param data
	 * @param current
	 * @return
	 */
	int trans(const PageReq &req, vector<StorageData> &data);

	/**
	 * 获取队列尾部数据
	 * @param queue
	 * @param data
	 * @return
	 */
	int get_back(const string &queue, QueueRsp &rsp);

	/**
	 * 获取队列头部数据
	 * @param queue
	 * @param data
	 * @return
	 */
	int get_front(const string &queue, QueueRsp &rsp);

	/**
	 * 队列是否有数据
	 * @param queue
	 * @param index
	 * @param has
	 * @return
	 */
	int get(const string &queue, int64_t index, QueueRsp &rsp);

	/**
	 * 关闭数据库
	 */
	void close();

protected:
	using onapply_type = std::function<void(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)>;
	using field_update_type = std::function<STORAGE_RT(JsonValuePtr &value, const Base::StorageUpdate &update)>;

	//为了保持住key, 不用直接用string 否则mac下, rocksdb::Slice莫名其妙内存被释放了, linux上没问题
	struct AutoSlice
	{
		AutoSlice(const char *buff, size_t len) : data(buff), length(len)
		{
		}

		~AutoSlice()
		{
			if(data)
			{
				delete data;
				data = NULL;
			}
			length = 0;
		}

		const char *data = NULL;
		size_t length = 0;
	};

	void open(const string &dbDir);
	string getDbDir() { return _raftDataDir + FILE_SEP + "rocksdb_data"; }

	string tableName(const string &table) { return "t-" + table; }

	int checkStorageData(rocksdb::ColumnFamilyHandle* handle, const StorageData &data);
	bool isExpire(TarsInputStream<> &is);
	void onSet(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onUpdate(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onUpdateBatch(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onSetBatch(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onDel(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onDelBatch(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onCreateTable(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);

	void writeBatch(rocksdb::WriteBatch &batch, int64_t appliedIndex);

	int onUpdateJson(rocksdb::WriteBatch &batch, const StorageJson &update);

	shared_ptr<AutoSlice> tokey(const StorageKey &key);
	shared_ptr<AutoSlice> tokeyUpper(const string &mkey);
	StorageKey keyto(const char *key, size_t length);

	rocksdb::ColumnFamilyHandle* getTable(const string &table);
	rocksdb::ColumnFamilyHandle* getQueue(const string &queue);

	STORAGE_RT updateStringReplace(JsonValuePtr &value, const Base::StorageUpdate &update);
	STORAGE_RT updateStringAdd(JsonValuePtr &value, const Base::StorageUpdate &update);
	STORAGE_RT updateNumberReplace(JsonValuePtr &value, const Base::StorageUpdate &update);
	STORAGE_RT updateNumberAdd(JsonValuePtr &value, const Base::StorageUpdate &update);
	STORAGE_RT updateNumberSub(JsonValuePtr &value, const Base::StorageUpdate &update);
	STORAGE_RT updateBooleanReplace(JsonValuePtr &value, const Base::StorageUpdate &update);
	STORAGE_RT updateBooleanReverse(JsonValuePtr &value, const Base::StorageUpdate &update);
	STORAGE_RT updateArrayReplace(JsonValuePtr &value, const Base::StorageUpdate &update);
	STORAGE_RT updateArrayAdd(JsonValuePtr &value, const Base::StorageUpdate &update);
	STORAGE_RT updateArraySub(JsonValuePtr &value, const Base::StorageUpdate &update);
	STORAGE_RT updateArrayAddNoRepeat(JsonValuePtr &value, const Base::StorageUpdate &update);

	string queueName(const string &table) { return "q-" + table; }
	void onCreateQueue(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onDeleteData(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onPushBack(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onPushFront(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onPopBack(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onPopFront(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);
	void onClearQueue(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback);

protected:
	string          _raftDataDir;
	rocksdb::DB     *_db = NULL;

	std::mutex		_mutex;
	unordered_map<string, rocksdb::ColumnFamilyHandle*> _column_familys;

	unordered_map<string, onapply_type>	_onApply;

	//字段更新机制
	map<tars::eJsonType, map<StorageOperator, field_update_type>>	_updateApply;
};


#endif //LIBRAFT_EXAMPLESTATEMACHINE_H
