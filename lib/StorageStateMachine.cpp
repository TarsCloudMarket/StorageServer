//
// Created by jarod on 2019-06-06.
//

#include "StorageStateMachine.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/compaction_filter.h"
#include "rocksdb/utilities/checkpoint.h"

#include "servant/Application.h"
#include "RaftNode.h"
#include <string.h>
#include "Storage.h"
#include "StorageServer.h"

using namespace Base;

const string StorageStateMachine::SET_TYPE  = "m1";
const string StorageStateMachine::BSET_TYPE = "m2";
const string StorageStateMachine::DEL_TYPE  = "m3";
const string StorageStateMachine::BDEL_TYPE = "m4";
const string StorageStateMachine::TABLE_TYPE = "m5";
const string StorageStateMachine::SET_JSON_TYPE = "m6";
const string StorageStateMachine::BSET_JSON_TYPE = "m7";
const string StorageStateMachine::DELETE_TABLE_TYPE = "m8";

const string StorageStateMachine::CREATE_QUEUE_TYPE = "q1";
const string StorageStateMachine::PUSH_QUEUE_TYPE = "q2";
const string StorageStateMachine::POP_QUEUE_TYPE = "q3";
const string StorageStateMachine::DELDATA_QUEUE_TYPE = "q4";
const string StorageStateMachine::DELETE_QUEUE_TYPE = "q5";
const string StorageStateMachine::SETDATA_QUEUE_TYPE = "q6";

const string StorageStateMachine::BATCH_DATA = "batch";

//////////////////////////////////////////////////////////////////////////////////////
class TTLCompactionFilter : public rocksdb::CompactionFilter
{
public:
	virtual bool Filter(int /*level*/, const rocksdb::Slice& /*key*/,
			const rocksdb::Slice& existing_value,
			std::string* /*new_value*/,
			bool* /*value_changed*/) const {

		int expireTime = 0;
		TarsInputStream<> is;
		is.setBuffer(existing_value.data(), existing_value.size());
		is.read(expireTime, 0, false);
		if(expireTime !=0 && expireTime < TNOW)
		{
//			LOG_CONSOLE_DEBUG << "TTLCompactionFilter:" << expireTime << endl;
			//过期了!
			return true;
		}

		return false;
	}

	const char *Name() const
	{
		return "Storage.TTLCompactionFilter";
	}

};
//////////////////////////////////////////////////////////////////////////////////////

class StorageKeyComparator : public rocksdb::Comparator
{
protected:
	void FindShortestSeparator(std::string *, const rocksdb::Slice &) const
	{
	}
	void FindShortSuccessor(std::string *) const
	{
	}

public:
	int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const
	{
		uint32_t amkeyLen = ntohl(*(uint32_t*)(a.data()));
		uint32_t aukeyLen = ntohl(*(uint32_t*)(a.data() + (amkeyLen + 1)+ sizeof(uint32_t) + 1));

		assert(amkeyLen + aukeyLen + 11 == a.size());

		uint32_t bmkeyLen = ntohl(*(uint32_t*)(b.data()));
		uint32_t bukeyLen = ntohl(*(uint32_t*)(b.data() + (bmkeyLen + 1)+ sizeof(uint32_t) + 1));
		assert(bmkeyLen + bukeyLen + 11 == b.size());

		//中间的分隔符
		unsigned char sepa = a[sizeof(uint32_t) + amkeyLen + 1];
		unsigned char sepb = b[sizeof(uint32_t) + bmkeyLen + 1];

		if(amkeyLen == bmkeyLen && bmkeyLen == 0)
		{
			//分隔符相同, 认为是相等的
			if(sepa == sepb)
			{
				return 0;
			}

			return sepa < sepb ? -1 : 1;
		}

		if(amkeyLen == 0)
		{
//			LOG_CONSOLE_DEBUG << (char)sepa << " " << (char)sepb << endl;
			if(sepa == sepb)
			{
				return 1;
			}

			return sepa < sepb ? -1 : 1;
		}

		if(bmkeyLen == 0)
		{
//			LOG_CONSOLE_DEBUG << (char)sepa << " " << (char)sepb << endl;
			if(sepa == sepb)
			{
				return -1;
			}

			return sepa < sepb ? -1 : 1;
		}

		int flag = TC_Port::strcmp(a.data() + sizeof(uint32_t), b.data() + sizeof(uint32_t));

		if(flag != 0)
		{
			return flag;
		}

		if(sepa != sepb)
		{
			return sepa < sepb ? -1 : 1;
		}

		return TC_Port::strcmp(a.data() + sizeof(uint32_t) + amkeyLen + 2 + sizeof(uint32_t) , b.data() + sizeof(uint32_t) + bmkeyLen + 2 + sizeof(uint32_t)) ;
	}

	// Ignore the following methods for now:
	const char *Name() const
	{
		return "Storage.StorageKeyComparator";
	}
};

//////////////////////////////////////////////////////////////////////////////////////

class QueueKeyComparator : public rocksdb::Comparator
{
protected:
	void FindShortestSeparator(std::string *, const rocksdb::Slice &) const
	{
	}
	void FindShortSuccessor(std::string *) const
	{
	}

public:
	int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const
	{
		int64_t index1 = *(int64_t*)(a.data());
		int64_t index2 = *(int64_t*)(b.data());

		if(index1 == index2)
		{
			return 0;
		}

		return index1 < index2 ? -1 : 1;
	}

	// Ignore the following methods for now:
	const char *Name() const
	{
		return "Storage.QueueKeyComparator";
	}
};

//////////////////////////////////////////////////////////////////////////////////////
StorageStateMachine::StorageStateMachine(const string &dataPath, StorageServer *server)
{
	_raftDataDir = dataPath;
	_server = server;

	_onApply[SET_TYPE] = std::bind(&StorageStateMachine::onSet, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[BSET_TYPE] = std::bind(&StorageStateMachine::onSetBatch, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[DEL_TYPE] = std::bind(&StorageStateMachine::onDel, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[BDEL_TYPE] = std::bind(&StorageStateMachine::onDelBatch, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[TABLE_TYPE] = std::bind(&StorageStateMachine::onCreateTable, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[SET_JSON_TYPE] = std::bind(&StorageStateMachine::onUpdate, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[BSET_JSON_TYPE] = std::bind(&StorageStateMachine::onUpdateBatch, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[DELETE_TABLE_TYPE] = std::bind(&StorageStateMachine::onDeleteTable, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

	_onApply[CREATE_QUEUE_TYPE] = std::bind(&StorageStateMachine::onCreateQueue, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[DELDATA_QUEUE_TYPE] = std::bind(&StorageStateMachine::onDeleteDataQueue, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[PUSH_QUEUE_TYPE] = std::bind(&StorageStateMachine::onPushQueue, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[POP_QUEUE_TYPE] = std::bind(&StorageStateMachine::onPopQueue, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[DELETE_QUEUE_TYPE] = std::bind(&StorageStateMachine::onDeleteQueue, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[SETDATA_QUEUE_TYPE] = std::bind(&StorageStateMachine::onSetDataQueue, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

	_onApply[BATCH_DATA] = std::bind(&StorageStateMachine::onBatch, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

	_updateApply[eJsonTypeString][SO_REPLACE] = std::bind(&StorageStateMachine::updateStringReplace, this, std::placeholders::_1, std::placeholders::_2);
	_updateApply[eJsonTypeString][SO_ADD] = std::bind(&StorageStateMachine::updateStringAdd, this, std::placeholders::_1, std::placeholders::_2);
	_updateApply[eJsonTypeNum][SO_REPLACE] = std::bind(&StorageStateMachine::updateNumberReplace, this, std::placeholders::_1, std::placeholders::_2);
	_updateApply[eJsonTypeNum][SO_ADD] = std::bind(&StorageStateMachine::updateNumberAdd, this, std::placeholders::_1, std::placeholders::_2);
	_updateApply[eJsonTypeNum][SO_SUB] = std::bind(&StorageStateMachine::updateNumberSub, this, std::placeholders::_1, std::placeholders::_2);
	_updateApply[eJsonTypeBoolean][SO_REPLACE] = std::bind(&StorageStateMachine::updateBooleanReplace, this, std::placeholders::_1, std::placeholders::_2);
	_updateApply[eJsonTypeBoolean][SO_REVERSE] = std::bind(&StorageStateMachine::updateBooleanReverse, this, std::placeholders::_1, std::placeholders::_2);
	_updateApply[eJsonTypeArray][SO_REPLACE] = std::bind(&StorageStateMachine::updateArrayReplace, this, std::placeholders::_1, std::placeholders::_2);
	_updateApply[eJsonTypeArray][SO_ADD] = std::bind(&StorageStateMachine::updateArrayAdd, this, std::placeholders::_1, std::placeholders::_2);
	_updateApply[eJsonTypeArray][SO_SUB] = std::bind(&StorageStateMachine::updateArraySub, this, std::placeholders::_1, std::placeholders::_2);
	_updateApply[eJsonTypeArray][SO_ADD_NO_REPEAT] = std::bind(&StorageStateMachine::updateArrayAddNoRepeat, this, std::placeholders::_1, std::placeholders::_2);

}

StorageStateMachine::~StorageStateMachine()
{
	close();
}

void StorageStateMachine::terminate()
{
	_server->terminate();
}

shared_ptr<StorageStateMachine::AutoSlice> StorageStateMachine::tokeyLower()
{
	uint32_t mkeyLen = 0;
	uint32_t ukeyLen = 0;

	size_t length = sizeof(mkeyLen) + 1 + 1 + sizeof(ukeyLen) + 1;
	const char *buff = new char[length];
	size_t pos = 0;
	memcpy((void*)buff, (const char*)&mkeyLen, sizeof(mkeyLen));
	pos += sizeof(mkeyLen);
	pos += 1;
	memcpy((void*)(buff + pos), ",", 1);
	pos += 1;

	memcpy((void*)(buff + pos), (const char*)&ukeyLen, sizeof(ukeyLen));
	pos += sizeof(ukeyLen);
	memcpy((void*)(buff + pos), "\0", 1);
	pos += 1;

	assert(pos == length);

	return std::make_shared<AutoSlice>(buff, length);
}

shared_ptr<StorageStateMachine::AutoSlice> StorageStateMachine::tokeyUpper()
{
	uint32_t mkeyLen = 0;
	uint32_t ukeyLen = 0;

	size_t length = sizeof(mkeyLen) + 1 + 1 + sizeof(ukeyLen) + 1;
	const char *buff = new char[length];
	size_t pos = 0;
	memcpy((void*)buff, (const char*)&mkeyLen, sizeof(mkeyLen));
	pos += sizeof(mkeyLen);
	pos += 1;
	memcpy((void*)(buff + pos), ".", 1);
	pos += 1;

	memcpy((void*)(buff + pos), (const char*)&ukeyLen, sizeof(ukeyLen));
	pos += sizeof(ukeyLen);
	memcpy((void*)(buff + pos), "\0", 1);
	pos += 1;

	assert(pos == length);

	return std::make_shared<AutoSlice>(buff, length);
}

shared_ptr<StorageStateMachine::AutoSlice> StorageStateMachine::tokeyUpper(const string &mkey)
{
	uint32_t mkeyLen = htonl((uint32_t)mkey.length());
	uint32_t ukeyLen = 0;

	size_t length = sizeof(mkeyLen) + (mkey.length() + 1) + 1 + sizeof(ukeyLen) + 1;
	const char *buff = new char[length];
	size_t pos = 0;
	memcpy((void*)buff, (const char*)&mkeyLen, sizeof(mkeyLen));
	pos += sizeof(mkeyLen);
	memcpy((void*)(buff + pos), mkey.data(), mkey.size()+1);
	pos +=  mkey.size()+1;
	memcpy((void*)(buff + pos), ".", 1);
	pos += 1;
	memcpy((void*)(buff + pos), (const char*)&ukeyLen, sizeof(ukeyLen));
	pos += sizeof(ukeyLen);
	memcpy((void*)(buff + pos), "\0", 1);
	pos += 1;

	assert(pos == length);

	return std::make_shared<AutoSlice>(buff, length);
}

shared_ptr<StorageStateMachine::AutoSlice> StorageStateMachine::tokey(const StorageKey &key)
{
	uint32_t mkeyLen = htonl((uint32_t)key.mkey.length());
	uint32_t ukeyLen = htonl((uint32_t)key.ukey.length());

	size_t length = sizeof(mkeyLen) + (key.mkey.length() + 1) + 1 + sizeof(ukeyLen) + (key.ukey.length() + 1);
	const char *buff = new char[length];
	size_t pos = 0;
	memcpy((void*)buff, (const char*)&mkeyLen, sizeof(mkeyLen));
	pos += sizeof(mkeyLen);
	memcpy((void*)(buff + pos), key.mkey.data(), key.mkey.size()+1);
	pos +=  key.mkey.size()+1;
	memcpy((void*)(buff + pos), "-", 1);
	pos += 1;
	memcpy((void*)(buff + pos), (const char*)&ukeyLen, sizeof(ukeyLen));
	pos += sizeof(ukeyLen);
	memcpy((void*)(buff + pos), key.ukey.data(), key.ukey.size()+1);
	pos += key.ukey.size()+1;

	assert(pos == length);
	return std::make_shared<AutoSlice>(buff, length);
}

StorageKey StorageStateMachine::keyto(const char *key, size_t length)
{
	uint32_t amkeyLen = ntohl(*(uint32_t*)(key));
	uint32_t aukeyLen = ntohl(*(uint32_t*)(key + sizeof(uint32_t) + (amkeyLen + 1) + 1));

	assert(sizeof(uint32_t)+(amkeyLen+1)+1+sizeof(uint32_t)+(aukeyLen+1) == length);

	StorageKey skey;
	skey.mkey = string(key + sizeof(uint32_t), amkeyLen);
	skey.ukey = string(key + sizeof(uint32_t) + (amkeyLen + 1) + 1 + sizeof(uint32_t), aukeyLen);

	return skey;
}

rocksdb::ColumnFamilyHandle* StorageStateMachine::getTable(const string &table)
{
	//防止和系统的default重名!
	string dTable = tableName(table);

	std::lock_guard<std::mutex> lock(_mutex);

	auto it = _column_familys.find(dTable);
	if(it != _column_familys.end())
	{
		return it->second;
	}

	TLOG_ERROR("no table:" << dTable << endl);
	return NULL;
}

rocksdb::ColumnFamilyHandle* StorageStateMachine::getQueue(const string &queue)
{
	//防止和系统的default重名!
	string dTable = queueName(queue);

	std::lock_guard<std::mutex> lock(_mutex);

	auto it = _column_familys.find(dTable);
	if(it != _column_familys.end())
	{
		return it->second;
	}

	TLOG_ERROR("no queue:" << dTable << endl);
	return NULL;
}

void StorageStateMachine::open(const string &dbDir)
{
	TLOG_DEBUG("db: " << dbDir << endl);

	tars::TC_File::makeDirRecursive(dbDir);

	// open rocksdb data dir
	rocksdb::Options options;
	options.create_if_missing = true;
	options.level_compaction_dynamic_level_bytes = true;
	options.periodic_compaction_seconds = 3600;

	std::vector<rocksdb::ColumnFamilyDescriptor> column_families;

	vector<string> columnFamilies;

	rocksdb::Status status = rocksdb::DB::ListColumnFamilies(options, dbDir, &columnFamilies);

	if (columnFamilies.empty())
	{
		status = rocksdb::DB::Open(options, dbDir, &_db);
		if (!status.ok())
		{
			throw std::runtime_error(status.ToString());
		}
	}
	else
	{
		std::vector<rocksdb::ColumnFamilyDescriptor> columnFamiliesDesc;
		for (auto &f : columnFamilies)
		{
			rocksdb::ColumnFamilyDescriptor c;

			c.name = f;

			if (c.name != "default" )
			{
				if(TC_Port::strncasecmp(c.name.c_str(), "t-", 2) == 0)
				{
					c.options.comparator = new StorageKeyComparator();
					c.options.compaction_filter = new TTLCompactionFilter();

					_tables.push_back(c.name.substr(2));

				}
				else
				{
					c.options.comparator = new QueueKeyComparator();
					c.options.compaction_filter = NULL;

					_queues.push_back(c.name.substr(2));
				}
			}

			columnFamiliesDesc.push_back(c);
		}

		std::vector<rocksdb::ColumnFamilyHandle *> handles;
		status = rocksdb::DB::Open(options, dbDir, columnFamiliesDesc, &handles, &_db);
		if (!status.ok())
		{
			TLOG_ERROR("Open " << dbDir << ", error:" << status.ToString() << endl);
			throw std::runtime_error(status.ToString());
		}

		for (auto &h : handles)
		{
			_column_familys[h->GetName()] = h;
		}
	}
}

void StorageStateMachine::close()
{
	if (_db) {

		for(auto e : _column_familys)
		{
			_db->DestroyColumnFamilyHandle(e.second);
		}
		_column_familys.clear();

		_db->Close();
		delete _db;
		_db = NULL;
	}
}

void StorageStateMachine::onBecomeLeader(int64_t term)
{
	TARS_NOTIFY_NORMAL("become leader, term:" + TC_Common::tostr(term));
	TLOG_DEBUG("term:" << term << endl);
}

void StorageStateMachine::onBecomeFollower()
{
	TARS_NOTIFY_NORMAL("become follower");

	TLOG_DEBUG("onBecomeFollower" << endl);
}

void StorageStateMachine::onStartElection(int64_t term)
{
	TARS_NOTIFY_NORMAL("start election");
}

void StorageStateMachine::onJoinCluster()
{
	TARS_NOTIFY_NORMAL("join cluster");
}

void StorageStateMachine::onLeaveCluster()
{
	TARS_NOTIFY_NORMAL("leave cluster");
}

void StorageStateMachine::onBeginSyncShapshot()
{
	TARS_NOTIFY_NORMAL("begin sync shapshot");
	TLOG_DEBUG("onBeginSyncShapshot" << endl);
}

void StorageStateMachine::onEndSyncShapshot()
{
	TARS_NOTIFY_NORMAL("end sync shapshot");
	TLOG_DEBUG("onEndSyncShapshot" << endl);
}

int64_t StorageStateMachine::onLoadData()
{
	TLOG_DEBUG("onLoadData" << endl);

	string dataDir = getDbDir(); //_raftDataDir + FILE_SEP + "rocksdb_data";

	TC_File::makeDirRecursive(dataDir);

	//把正在使用的db关闭
	close();

	open(dataDir);

	int64_t lastAppliedIndex = 0;

	string value;
	auto s = _db->Get(rocksdb::ReadOptions(), "lastAppliedIndex", &value);
	if(s.ok())
	{
		lastAppliedIndex = *(int64_t*)value.c_str();

	}
	else if(s.IsNotFound())
	{
		lastAppliedIndex = 0;
	}
	else if(!s.ok())
	{
		TLOG_ERROR("Get lastAppliedIndex error!" << s.ToString() << endl);
		terminate();
	}

	TLOG_DEBUG("lastAppliedIndex:" << lastAppliedIndex << endl);

	return lastAppliedIndex;
}

void StorageStateMachine::onSaveSnapshot(const string &snapshotDir)
{
	TLOG_DEBUG("onSaveSnapshot:" << snapshotDir << endl);

	rocksdb::Checkpoint *checkpoint = NULL;

	rocksdb::Status s = rocksdb::Checkpoint::Create(_db, &checkpoint);

	assert(s.ok());

	checkpoint->CreateCheckpoint(snapshotDir);

	delete checkpoint;
}

bool StorageStateMachine::onLoadSnapshot(const string &snapshotDir)
{
	string dataDir = getDbDir(); //_raftDataDir + FILE_SEP + "rocksdb_data";

	//把正在使用的db关闭
	close();

	//非启动时(安装节点)
	TC_File::removeFile(dataDir, true);
	TC_File::makeDirRecursive(dataDir);

	TLOG_DEBUG("copy: " << snapshotDir << " to " << dataDir << endl);

	//把快照文件copy到数据目录
	TC_File::copyFile(snapshotDir, dataDir);

	onLoadData();
	
    return true;
}

void StorageStateMachine::onApply(const char *buff, size_t length, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	TarsInputStream<> is;
	is.setBuffer(buff, length);

	string type;
	is.read(type, 0, true);

	TLOG_DEBUG(type << ", appliedIndex:" << appliedIndex << ", size:" << _onApply.size() << endl);

	auto it = _onApply.find(type);

	if(it == _onApply.end())
	{
		TLOG_ERROR(type << " not known, appliedIndex:" << appliedIndex << ", size:" << _onApply.size() << endl);

		assert(it != _onApply.end());

		TC_Common::msleep(100);
		terminate();

		return;
	}

	it->second(is, appliedIndex, callback);
}

int StorageStateMachine::has(const StorageKey &skey)
{
	auto handle = getTable(skey.table);
	if(!handle)
	{
		return S_TABLE_NOT_EXIST;
	}

	auto key = tokey(skey);
	std::string value;
	rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), handle, rocksdb::Slice(key->data, key->length), &value);
	if (s.ok())
	{
		TarsInputStream<> is;
		is.setBuffer(value.c_str(), value.length());

		if(isExpire(is))
		{
			return S_NO_DATA;
		}
	}
	else if (s.IsNotFound()) {
		return S_NO_DATA;
	}
	else
	{
		TLOG_ERROR("Get: " << key << ", error:" << s.ToString() << endl);

		terminate();

		return S_ERROR;
	}

	return S_OK;
}

bool StorageStateMachine::isExpire(TarsInputStream<> &is)
{
	int expireTime = 0;
	is.read(expireTime, 0, false);

	is.reset();

	//过期了!
	if(expireTime !=0 && expireTime < TNOW)
	{
		TLOG_ERROR("expireTime :" << expireTime << ", TNOW:" << TNOW << endl);

		return true;
	}

	return false;
}

int StorageStateMachine::get(const StorageKey &skey, StorageValue &data)
{
	TLOG_DEBUG("table:" << skey.table << ", mkey:" << skey.mkey << ", ukey:" << skey.ukey << endl);

	auto handle = getTable(skey.table);
	if(!handle)
	{
		TLOG_ERROR("table:" << skey.table << ", mkey:" << skey.mkey << ", ukey:" << skey.ukey << ", table not exists!" << endl);

		return S_TABLE_NOT_EXIST;
	}

	auto key = tokey(skey);
	std::string value;
	rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), handle, rocksdb::Slice(key->data, key->length), &value);
	if (s.ok())
	{
		TarsInputStream<> is;
		is.setBuffer(value.c_str(), value.length());

		if(isExpire(is))
		{
			TLOG_ERROR("table:" << skey.table << ", mkey:" << skey.mkey << ", ukey:" << skey.ukey << ", expire" << endl);
			return S_NO_DATA;
		}

		data.readFrom(is);
	}
	else if (s.IsNotFound()) {
		TLOG_ERROR("table:" << skey.table << ", mkey:" << skey.mkey << ", ukey:" << skey.ukey << ", no found" << endl);

		return S_NO_DATA;
	}
	else
	{
		TLOG_ERROR("Get: " << skey.table << ", mkey:" << skey.mkey << ", ukey:" << skey.ukey << ", error:" << s.ToString() << endl);

		terminate();
		return S_ERROR;
	}

	return S_OK;
}

int StorageStateMachine::hasBatch(const vector<StorageKey> &skey, map<StorageKey, int> &data)
{
	data.clear();

	vector<shared_ptr<AutoSlice>> v;
	vector<rocksdb::Slice> keys;
	std::vector<rocksdb::ColumnFamilyHandle*> columns;
	vector<std::string> value;

	v.reserve(skey.size());
	keys.reserve(skey.size());
	columns.reserve(skey.size());
	value.reserve(skey.size());

	for(size_t i = 0; i < skey.size(); i++)
	{
		v.push_back(tokey(skey[i]));
		keys.push_back(rocksdb::Slice(v[i]->data, v[i]->length));

		auto handle = getTable(skey[i].table);
		if(!handle)
		{
			TLOG_ERROR("table:" << skey[i].table << ", mkey:" << skey[i].mkey << ", ukey:" << skey[i].ukey << ", table not exists" << endl);

			return S_TABLE_NOT_EXIST;
		}

		columns.push_back(handle);
	}

	vector<rocksdb::Status> status = _db->MultiGet(rocksdb::ReadOptions(), columns, keys, &value);

	for(size_t i = 0; i < status.size(); i++)
	{
		if (status[i].ok())
		{
			TarsInputStream<> is;
			is.setBuffer(value[i].c_str(), value[i].length());

			if(isExpire(is))
			{
				data[skey[i]] = S_NO_DATA;
			}
			else
			{
				data[skey[i]] = S_OK;
			}
		}
		else if (status[i].IsNotFound()) {
			data[skey[i]] = S_NO_DATA;
		}
		else
		{
			TLOG_ERROR("has Batch, error:" << status[i].ToString() << endl);
			data[skey[i]] = S_ERROR;
		}
	}

	return S_OK;
}


int StorageStateMachine::get(const vector<StorageKey> &skey, vector<StorageData> &data)
{
	data.clear();

	vector<shared_ptr<AutoSlice>> v;
	vector<rocksdb::Slice> keys;
	std::vector<rocksdb::ColumnFamilyHandle*> columns;
	vector<std::string> value;

	v.reserve(skey.size());
	keys.reserve(skey.size());
	columns.reserve(skey.size());
	value.reserve(skey.size());

	for(size_t i = 0; i < skey.size(); i++)
	{
		v.push_back(tokey(skey[i]));
		keys.push_back(rocksdb::Slice(v[i]->data, v[i]->length));

		auto handle = getTable(skey[i].table);
		if(!handle)
		{
			TLOG_ERROR("table:" << skey[i].table << ", mkey:" << skey[i].mkey << ", ukey:" << skey[i].ukey << ", table not exists" << endl);

			return S_TABLE_NOT_EXIST;
		}

		columns.push_back(handle);
	}

	vector<rocksdb::Status> status = _db->MultiGet(rocksdb::ReadOptions(), columns, keys, &value);

	for(size_t i = 0; i < status.size(); i++)
	{
		if (status[i].ok())
		{
			TarsInputStream<> is;
			is.setBuffer(value[i].c_str(), value[i].length());

			StorageData d;
			d.skey = skey[i];

			if(isExpire(is))
			{
				d.ret = S_NO_DATA;
			}
			else
			{
				d.ret = S_OK;
				d.svalue.readFrom(is);
			}

			data.push_back(d);
		}
		else if (status[i].IsNotFound()) {

			StorageData d;
			d.ret = S_NO_DATA;
			d.skey = skey[i];

			data.push_back(d);

		}
		else
		{
			TLOG_ERROR("Get Batch, error:" << status[i].ToString() << endl);
			StorageData d;
			d.ret = S_ERROR;
			d.skey = skey[i];

			data.push_back(d);
		}
	}

	return S_OK;
}

int StorageStateMachine::trans(const PageReq &req, vector<StorageData> &data)
{
	data.clear();

	auto handle = getTable(req.skey.table);
	if(!handle)
	{
		TLOG_ERROR("table:" << req.skey.table << ", mkey:" << req.skey.mkey << ", ukey:" << req.skey.ukey << ", table not exists" << endl);

		return S_TABLE_NOT_EXIST;
	}

	auto_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions(), handle));

	if(req.forward)
	{
		shared_ptr<AutoSlice> k;

		if(!req.over)
		{
			k = tokey(req.skey);
		}
		else
		{
			if(!req.skey.mkey.empty())
			{
				k = tokey(req.skey);
			}
			else
			{
				k = tokeyLower();
			}
		}

		rocksdb::Slice key(k->data, k->length);

		//从小到大
		it->Seek(key);
		while(it->Valid() && (req.limit < 0 || data.size() < req.limit))
		{
			StorageData d;
			d.skey = keyto(it->key().data(), it->key().size());

			if(d.skey.mkey != req.skey.mkey && !req.over)
			{
				//跨mkey了
				break;
			}

			if(!req.include && req.skey.mkey == d.skey.mkey && req.skey.ukey == d.skey.ukey)
			{
				//不包含检索的第一个数据
				it->Next();
				continue;
			}

			d.skey.table = req.skey.table;

			TarsInputStream<> is;
			is.setBuffer(it->value().data(), it->value().size());

			if(!isExpire(is))
			{
				d.svalue.readFrom(is);
				data.push_back(d);
			}

			it->Next();
		}

	}
	else
	{
		shared_ptr<AutoSlice> k;

		if(!req.over)
		{
			if (!req.skey.ukey.empty())
			{
				k = tokey(req.skey);
			}
			else
			{
				k = tokeyUpper(req.skey.mkey);
			}
		}
		else
		{
			if (!req.skey.mkey.empty())
			{
				if (!req.skey.ukey.empty())
				{
					k = tokey(req.skey);
				}
				else
				{
					k = tokeyUpper(req.skey.mkey);
				}
			}
			else
			{
				k = tokeyUpper();
			}
		}

		//从大到小
		it->SeekForPrev(rocksdb::Slice(k->data, k->length));

		while(it->Valid() && (req.limit < 0 || data.size() < req.limit))
		{
			StorageData d;
			d.skey = keyto(it->key().data(), it->key().size());

			if(d.skey.mkey != req.skey.mkey && !req.over)
			{
				//跨mkey了
				break;
			}

			if(!req.include && req.skey.mkey == d.skey.mkey && req.skey.ukey == d.skey.ukey)
			{
				//不包含检索的第一个数据
				it->Prev();
				continue;
			}

			d.skey.table = req.skey.table;

			TarsInputStream<> is;
			is.setBuffer(it->value().data(), it->value().size());

			if(!isExpire(is))
			{
				d.svalue.readFrom(is);
				data.push_back(d);
			}

			it->Prev();
		}
	}

	TLOG_DEBUG("table:" << req.skey.table << ", mkey:" << req.skey.mkey << ", ukey:" << req.skey.ukey << ", forward:" << req.forward << ", limit:" << req.limit << ", result size: " << data.size() << endl);

	return S_OK;
}

int StorageStateMachine::checkStorageData(rocksdb::ColumnFamilyHandle* handle, const StorageData &sdata)
{
	if(sdata.svalue.version != 0 || sdata.svalue.timestamp != 0)
	{
		std::string value;
		auto key = tokey(sdata.skey);
		rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), handle, rocksdb::Slice(key->data, key->length), &value);
		if (s.ok())
		{
			StorageValue data;

			TarsInputStream<> is;
			is.setBuffer(value.c_str(), value.length());

			data.readFrom(is);

			if(sdata.svalue.version != 0 && sdata.svalue.version != data.version)
			{
				return S_VERSION;
			}

			if(sdata.svalue.timestamp != 0 && sdata.svalue.timestamp < data.timestamp)
			{
				return S_TIMESTAMP;
			}
		}
		else if(s.IsNotFound())
		{
			return S_OK;
		}
		else
		{
			TLOG_ERROR("Get: " << sdata.skey.mkey + "-" + sdata.skey.ukey << ", error:" << s.ToString() << endl);
			terminate();
			return S_ERROR;
		}
	}

	return S_OK;

}

void StorageStateMachine::writeBatch(rocksdb::WriteBatch &batch, int64_t appliedIndex)
{
	batch.Put("lastAppliedIndex", rocksdb::Slice((const char *)&appliedIndex, sizeof(appliedIndex)));

	rocksdb::WriteOptions wOption;
	wOption.sync = false;

	auto s = _db->Write(wOption, &batch);

	if(!s.ok())
	{
		TLOG_ERROR("writeBatch error!" << endl);
		terminate();
	}
}

void StorageStateMachine::onDeleteTable(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	string table;
	is.read(table, 1, false);

	TLOG_DEBUG("table:" << table << endl);

	auto handle = getTable(table);

	if(handle)
	{
		{
			std::lock_guard<std::mutex> lock(_mutex);

			_column_familys.erase(handle->GetName());

			_tables.erase(std::remove(_tables.begin(), _tables.end(), handle->GetName().substr(2)), _tables.end());
		}

		_db->DropColumnFamily(handle);

		_db->DestroyColumnFamilyHandle(handle);
	}

	if(callback)
	{
		Storage::async_response_deleteTable(callback->getCurrentPtr(), S_OK);
	}
}

void StorageStateMachine::onSet(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	StorageData data;

	is.read(data, 1, false);

	TLOG_DEBUG("table:" << data.skey.table << ", mkey:" << data.skey.mkey << ", ukey:" << data.skey.ukey
	<< ", version:" << data.svalue.version << ", timestamp:" << data.svalue.timestamp << ", expireTime:" << data.svalue.expireTime
		<< ", appliedIndex:" << appliedIndex << ", buffer size:" << data.svalue.data.size() << endl);

	if(data.svalue.expireTime > 0)
	{
		data.svalue.expireTime += TNOW;
	}

	int ret;

	rocksdb::WriteBatch batch;

	auto handle = getTable(data.skey.table);

	if(!handle)
	{
		TLOG_ERROR("table:" << data.skey.table << ", mkey:" << data.skey.mkey << ", ukey:" << data.skey.ukey << ", table not exists" << endl);

		ret = S_TABLE_NOT_EXIST;
	}
	else
	{
		ret = checkStorageData(handle, data);

		if(ret == S_OK)
		{
			TarsOutputStream<> buff;
			while(data.svalue.version == 0)
			{
				++data.svalue.version;
			}
			data.svalue.writeTo(buff);

			auto key = tokey(data.skey);

			batch.Put(handle, rocksdb::Slice(key->data, key->length), rocksdb::Slice(buff.getBuffer(), buff.getLength()));
		}
	}

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_set(callback->getCurrentPtr(), ret);
	}
}

int StorageStateMachine::onUpdateJson(rocksdb::WriteBatch &batch, const StorageJson &update, map<rocksdb::ColumnFamilyHandle*, map<string, pair<StorageValue, JsonValueObjPtr>>> &result)
{
	STORAGE_RT ret = S_OK;

	auto handle = getTable(update.skey.table);

	assert(handle);

	JsonValueObjPtr json;

	auto key = tokey(update.skey);

	string skey = string(key->data, key->length);
	auto it = result[handle].find(skey);

	if (it == result[handle].end())
	{
		std::string value;

		rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), handle, rocksdb::Slice(key->data, key->length), &value);
		if (!s.ok() && !s.IsNotFound())
		{
			TLOG_ERROR("Get: " << update.skey.mkey + "-" + update.skey.ukey << ", error:" << s.ToString() << endl);

			ret = S_ERROR;

			//这种情况服务数据出严重问题了!!!
			terminate();
			return S_ERROR;
		}

		StorageValue data;

		if (s.ok())
		{
			TarsInputStream<> is;
			is.setBuffer(value.c_str(), value.length());
			data.readFrom(is);

			json = JsonValueObjPtr::dynamicCast(TC_Json::getValue(data.data));

			if (!json)
			{
				TLOG_ERROR(update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey
											 << ", parse to json error." << endl);

				ret = S_JSON_VALUE_NOT_JSON;
			}
		}
		else
		{
			data.version = 0;
			data.expireTime = 0;
			data.timestamp = 0;
			json = new JsonValueObj();
		}

		result[handle][skey] = std::make_pair(data, json);

	}
	else
	{
		json = it->second.second;
	}

	if (ret != S_OK)
	{
		return ret;
	}

	for (auto& e: update.supdate)
	{
		auto it = json->value.find(e.field);
		if (it != json->value.end())
		{
			auto tIt = this->_updateApply.find(it->second->getType());
			if (tIt != this->_updateApply.end())
			{
				auto rIt = tIt->second.find(e.op);

				if (rIt != tIt->second.end())
				{
					ret = rIt->second(it->second, e);

					if (ret != S_OK)
					{
						TLOG_ERROR(
								update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey
												  << ", field:" << e.field << " , op:" << etos(e.op) << ", ret:"
												  << etos(ret) << endl);
						break;
					}
				}
				else
				{
					ret = Base::S_JSON_OPERATOR_NOT_SUPPORT;

					TLOG_ERROR(update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey
												 << ", field:" << e.field << " , op:" << etos(e.op) << ", ret:"
												 << etos(ret) << endl);

					break;
				}
			}
		}
		else if (!e.def.empty())
		{
			JsonValuePtr value;

			switch (e.type)
			{
			case Base::FT_INTEGER:
				value = new JsonValueNum(TC_Common::strto<int64_t>(e.def), true);
				break;
			case Base::FT_DOUBLE:
				value = new JsonValueNum(TC_Common::strto<double>(e.def), false);
				break;
			case Base::FT_STRING:
				value = new JsonValueString(e.def);
				break;
			case Base::FT_BOOLEAN:
				value = new JsonValueBoolean(TC_Port::strncasecmp(e.def.c_str(), "true", e.def.size()) == 0);
				break;
			case Base::FT_ARRAY:
			{
				value = JsonValueArrayPtr::dynamicCast(TC_Json::getValue(e.def));
				break;
			}
			default:
				ret = S_JSON_VALUE_TYPE_ERROR;
				break;
			}

			if (ret != S_OK)
			{
				break;
			}

			if (value)
			{
				json->value[e.field] = value;
			}
		}
		else
		{
			ret = S_JSON_FIELD_NOT_EXITS;
			TLOG_ERROR(update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey
										 << ", field:" << e.field << " in not exists, op:" << etos(e.op) << ", ret:"
										 << etos(ret) << endl);
			break;
		}


	}

	return ret;
}

void StorageStateMachine::onUpdate(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	vector<StorageJson> updates;
	updates.resize(1);

	is.read(updates[0], 1, false);

	TLOG_DEBUG("table:" << updates[0].skey.table << ", mkey:" << updates[0].skey.mkey << ", ukey:" << updates[0].skey.ukey
						<< ", appliedIndex:" << appliedIndex << ", update:" << TC_Common::tostr(updates[0].supdate.begin(), updates[0].supdate.end(), " ") << endl);

	rocksdb::WriteBatch batch;

	int ret = updateBatch(batch, updates);

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_update(callback->getCurrentPtr(), ret);
	}
}

STORAGE_RT StorageStateMachine::updateBatch(rocksdb::WriteBatch &batch, const vector<StorageJson> &updates)
{
	int ret = S_OK;

	for(auto &update : updates)
	{
		auto handle = getTable(update.skey.table);

		if(!handle)
		{
			TLOG_ERROR("table:" << update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey << ", table not exists" << endl);

			ret = S_TABLE_NOT_EXIST;
		}
	}

	if(ret != S_OK)
	{
		return (STORAGE_RT)ret;
	}

	map<rocksdb::ColumnFamilyHandle*, map<string, pair<StorageValue, JsonValueObjPtr>>> result;

	for(auto &update : updates)
	{
		ret = onUpdateJson(batch, update, result);

		if(ret != S_OK)
		{
			break;
		}
	}

	if(ret == S_OK)
	{
		for(auto data : result)
		{
			for(auto it : data.second)
			{
				TC_Json::writeValue(it.second.second, it.second.first.data);

				if (it.second.first.expireTime > 0)
				{
					it.second.first.expireTime += TNOW;
				}

				TarsOutputStream<> buff;
				while (it.second.first.version == 0)
				{
					++it.second.first.version;
				}

				it.second.first.writeTo(buff);

				batch.Put(data.first, rocksdb::Slice(it.first.c_str(), it.first.length()),
						rocksdb::Slice(buff.getBuffer(), buff.getLength()));
			}
		}
	}

	return (STORAGE_RT)ret;
}

void StorageStateMachine::onUpdateBatch(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	vector<StorageJson> updates;

	is.read(updates, 1, false);

	rocksdb::WriteBatch batch;

	STORAGE_RT ret = updateBatch(batch, updates);

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_update(callback->getCurrentPtr(), ret);
	}
}

STORAGE_RT StorageStateMachine::setBatch(rocksdb::WriteBatch &batch, const vector<StorageData> &vdata, map<StorageKey, int> &rsp)
{
	for(size_t i = 0; i < vdata.size(); i++)
	{
		StorageData& data = const_cast<StorageData&>(vdata[i]);

		auto handle = getTable(data.skey.table);

		if (!handle)
		{
			return Base::S_TABLE_NOT_EXIST;
		}
	}

	for(size_t i = 0; i < vdata.size(); i++)
	{
		StorageData &data = const_cast<StorageData&>(vdata[i]);

		auto handle = getTable(data.skey.table);

		assert(handle);

		int ret = checkStorageData(handle, data);

		if(ret != S_OK)
		{
			rsp[data.skey] = ret;
		}
		else
		{
			if (data.svalue.expireTime > 0)
			{
				data.svalue.expireTime += TNOW;
			}

			TarsOutputStream<> buff;
			while (data.svalue.version == 0)
			{
				++data.svalue.version;
			}

			data.svalue.writeTo(buff);

			auto key = tokey(data.skey);
			batch.Put(handle, rocksdb::Slice(key->data, key->length),
					rocksdb::Slice(buff.getBuffer(), buff.getLength()));

			rsp[data.skey] = S_OK;
		}
	}
	return S_OK;
}

void StorageStateMachine::onSetBatch(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	vector<StorageData> vdata;

	is.read(vdata, 1, false);

	TLOG_DEBUG("appliedIndex:" << appliedIndex << ", data size:" << vdata.size() << endl);

	map<StorageKey, int> rsp;

	int bret = S_OK;

	rocksdb::WriteBatch batch;

	bret = setBatch(batch, vdata, rsp);

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ",  response ret:" << etos((STORAGE_RT)bret) << ", size:" << rsp.size() << endl);

		Storage::async_response_setBatch(callback->getCurrentPtr(), bret, rsp);
	}
}

void StorageStateMachine::onDel(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	StorageKey skey;

	is.read(skey, 1, false);

	TLOG_DEBUG("table:" << skey.table << ", mkey:" << skey.mkey << ", ukey:" << skey.ukey << endl);

	int ret = S_OK;
	rocksdb::WriteBatch batch;

	auto key = tokey(skey);

	if(!skey.ukey.empty())
	{
		batch.Delete(getTable(skey.table), rocksdb::Slice(key->data, key->length));
	}
	else
	{
		auto upperKey = tokeyUpper(skey.mkey);
		batch.DeleteRange(getTable(skey.table), rocksdb::Slice(key->data, key->length), rocksdb::Slice(upperKey->data, upperKey->length));
	}

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ",  response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_set(callback->getCurrentPtr(), ret);
	}
}

void StorageStateMachine::onDelBatch(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	vector<StorageKey> skeys;

	is.read(skeys, 1, false);

	TLOG_DEBUG("delete size:" << skeys.size() << endl);

	int ret = S_OK;
	rocksdb::WriteBatch batch;

	for(auto &skey : skeys)
	{
		auto key = tokey(skey);
		auto handle = getTable(skey.table);
		if(!handle)
		{
			continue;
		}

		if(!skey.ukey.empty())
		{
			batch.Delete(handle, rocksdb::Slice(key->data, key->length));
		}
		else
		{
			auto upperKey = tokeyUpper(skey.mkey);
			batch.DeleteRange(getTable(skey.table), rocksdb::Slice(key->data, key->length), rocksdb::Slice(upperKey->data, upperKey->length));
		}

	}

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_set(callback->getCurrentPtr(), ret);
	}
}

void StorageStateMachine::onCreateTable(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	string table;
	is.read(table, 1, false);

	table = tableName(table);

	TLOG_DEBUG("table name:" << table << ", appliedIndex:" << appliedIndex << endl);

	int ret = S_OK;
	rocksdb::WriteBatch batch;

	writeBatch(batch, appliedIndex);

	bool exist = false;

	{
		std::lock_guard<std::mutex> lock(_mutex);
		if(_column_familys.find(table) != _column_familys.end())
		{
			exist = true;
		}
	}

	TLOG_ERROR("table name:" << table << ", exists:" << exist << endl);

	if(exist)
	{
		if(callback)
		{
			//如果客户端请求过来的, 直接回包
			//如果是其他服务器同步过来, 不用回包了
			TLOG_DEBUG("table name:" << table << ", appliedIndex:" << appliedIndex << ", response succ S_TABLE_EXIST" << endl);

			Storage::async_response_createTable(callback->getCurrentPtr(), S_TABLE_EXIST);
		}
	}
	else
	{
		rocksdb::ColumnFamilyHandle* handle;
		rocksdb::ColumnFamilyOptions options;
		options.comparator = new StorageKeyComparator();
		options.compaction_filter = new TTLCompactionFilter();

		auto status = _db->CreateColumnFamily(options, table, &handle);
		if (!status.ok())
		{
			TLOG_ERROR("CreateColumnFamily error:" << status.ToString() << endl);
			throw std::runtime_error(status.ToString());
		}

		{
			std::lock_guard<std::mutex> lock(_mutex);
			_column_familys[table] = handle;
			_tables.push_back(table.substr(2));
		}
		if(callback)
		{
			TLOG_DEBUG("table name:" << table << ", appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

			Storage::async_response_createTable(callback->getCurrentPtr(), ret);
		}
	}
}

STORAGE_RT StorageStateMachine::updateStringReplace(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueStringPtr p = JsonValueStringPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	p->value = update.value;

	return S_OK;
}

STORAGE_RT StorageStateMachine::updateStringAdd(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueStringPtr p = JsonValueStringPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	p->value += update.value;

	return S_OK;
}

STORAGE_RT StorageStateMachine::updateNumberReplace(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueNumPtr p = JsonValueNumPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	if (update.type == Base::FT_INTEGER)
	{
		p->isInt = true;
		p->lvalue = TC_Common::strto<int64_t>(update.value);
	}
	else
	{
		p->isInt = false;
		p->value = TC_Common::strto<double>(update.value);
	}

	return S_OK;
}

STORAGE_RT StorageStateMachine::updateNumberAdd(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueNumPtr p = JsonValueNumPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	if (update.type == Base::FT_INTEGER)
	{
		if(p->isInt)
		{
			p->lvalue += TC_Common::strto<int64_t>(update.value);
		}
		else
		{
			p->value += TC_Common::strto<double>(update.value);
		}
	}
	else
	{
		p->value += TC_Common::strto<double>(update.value);
		p->isInt = false;
	}

	return S_OK;
}

STORAGE_RT StorageStateMachine::updateNumberSub(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueNumPtr p = JsonValueNumPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	if (update.type == Base::FT_INTEGER)
	{
		if(p->isInt)
		{
			p->lvalue -= TC_Common::strto<int64_t>(update.value);
		}
		else
		{
			p->value -= TC_Common::strto<double>(update.value);
		}
	}
	else
	{
		p->value -= TC_Common::strto<double>(update.value);
		p->isInt = false;
	}

	return S_OK;
}

STORAGE_RT StorageStateMachine::updateBooleanReplace(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueBooleanPtr p = JsonValueBooleanPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	if (TC_Port::strcasecmp(update.value.c_str(), "true") == 0)
	{
		p->value = true;
	}
	else
	{
		p->value = false;
	}

	return S_OK;
}

STORAGE_RT StorageStateMachine::updateBooleanReverse(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueBooleanPtr p = JsonValueBooleanPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	p->value = !p->value;

	return S_OK;

}

STORAGE_RT StorageStateMachine::updateArrayReplace(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueArrayPtr p = JsonValueArrayPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	JsonValueArrayPtr arrayPtr = JsonValueArrayPtr::dynamicCast(TC_Json::getValue(update.value));

	p->value = arrayPtr->value;

	return S_OK;
}

STORAGE_RT StorageStateMachine::updateArrayAdd(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueArrayPtr p = JsonValueArrayPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	switch(update.type)
	{
	case Base::FT_INTEGER:
		p->value.push_back(new JsonValueNum(TC_Common::strto<int64_t>(update.value), true));
		break;
	case Base::FT_DOUBLE:
		p->value.push_back(new JsonValueNum(TC_Common::strto<double>(update.value), false));
		break;
	case Base::FT_STRING:
		p->value.push_back(new JsonValueString(update.value));
		break;
	case Base::FT_BOOLEAN:
		p->value.push_back(new JsonValueBoolean(TC_Port::strncasecmp(update.value.c_str(),"true", update.value.size()) == 0));
		break;
	case Base::FT_ARRAY:
	{
		JsonValueArrayPtr arrayPtr = JsonValueArrayPtr::dynamicCast(
				TC_Json::getValue(update.value));

		p->value.insert(p->value.end(), arrayPtr->value.begin(), arrayPtr->value.end());
		break;
	}
	default:
		return S_JSON_VALUE_TYPE_ERROR;
	}

	return  S_OK;
}

STORAGE_RT StorageStateMachine::updateArraySub(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueArrayPtr p = JsonValueArrayPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	switch(update.type)
	{
	case Base::FT_INTEGER:
	case Base::FT_DOUBLE:
		while(true)
		{
			auto it = p->find(eJsonTypeNum, update.value);

			if (it != p->value.end())
			{
				p->value.erase(it);
			}
			else
			{
				break;
			}
		}
		break;
	case Base::FT_STRING:
		while(true)
		{
			auto it = p->find(eJsonTypeString, update.value);

			if (it != p->value.end())
			{
				p->value.erase(it);
			}
			else
			{
				break;
			}
		}
		break;
	case Base::FT_BOOLEAN:
	{
		while (true)
		{
			auto it = p->find(eJsonTypeBoolean, update.value);

			if (it != p->value.end())
			{
				p->value.erase(it);
			}
			else
			{
				break;
			}
		}
		break;
	}
	case Base::FT_ARRAY:
	{
		JsonValueArrayPtr arrayPtr = JsonValueArrayPtr::dynamicCast(TC_Json::getValue(update.value));
		if(!arrayPtr)
		{
			return S_JSON_VALUE_TYPE_ERROR;
		}

		while (true)
		{
			for(auto item : arrayPtr->value)
			{
				auto it = p->find(item);
				if(it != p->value.end())
				{
					p->value.erase(it);
				}
			}
		}
		break;
	}
	default:
		return S_JSON_VALUE_TYPE_ERROR;
	}

	return S_OK;
}

STORAGE_RT StorageStateMachine::updateArrayAddNoRepeat(JsonValuePtr &value, const Base::StorageUpdate &update)
{
	JsonValueArrayPtr p = JsonValueArrayPtr::dynamicCast(value);
	if(!p)
	{
		return S_JSON_FIELD_TYPE_ERROR;
	}

	switch(update.type)
	{
	case Base::FT_INTEGER:
	{
		auto it = p->find(eJsonTypeNum, update.value);
		if (it == p->value.end())
		{
			p->value.push_back(new JsonValueNum(TC_Common::strto<int64_t>(update.value), true));
		}
		break;
	}
	case Base::FT_DOUBLE:
	{
		auto it = p->find(eJsonTypeNum, update.value);
		if (it == p->value.end())
		{
			p->value.push_back(new JsonValueNum(TC_Common::strto<double>(update.value), false));
		}
		break;
	}
	case Base::FT_STRING:
	{
		auto it = p->find(eJsonTypeString, update.value);
		if (it == p->value.end())
		{
			p->value.push_back(new JsonValueString(update.value));
		}
		break;
	}
	case Base::FT_BOOLEAN:
	{
		auto it = p->find(eJsonTypeBoolean, update.value);
		if (it == p->value.end())
		{
			p->value.push_back(new JsonValueBoolean(TC_Port::strncasecmp(update.value.c_str(),"true", update.value.length()) == 0));
		}
		break;
	}
	case Base::FT_ARRAY:
	{
		JsonValueArrayPtr arrayPtr = JsonValueArrayPtr::dynamicCast(TC_Json::getValue(update.value));
		if(!arrayPtr)
		{
			return S_JSON_VALUE_TYPE_ERROR;
		}

		for(auto array : arrayPtr->value)
		{
			auto it = p->find(array);
			if(it == p->value.end())
			{
				p->value.push_back(array);
			}
		}

		break;
	}
	default:
	{
		return S_JSON_VALUE_TYPE_ERROR;
	}
	}

	return S_OK;
}

/////////////////////////////////////////////////////////////////////////////////////////////

void StorageStateMachine::get_data(const string &queue, int64_t index, const char *buff, size_t length, vector<QueueRsp> &rsp)
{
	TarsInputStream<> is;
	is.setBuffer(buff, length);
	QueueData qd;
	qd.readFrom(is);

	if(qd.expireTime <= 0 || (qd.expireTime > 0 && qd.expireTime > TNOW))
	{
		QueueRsp data;
		data.queue = queue;
		data.index = index;
		data.expireTime = qd.expireTime;

		//未过期
		data.data.swap(qd.data);

		rsp.push_back(data);
	}
	else
	{
		//队列数据超时了, 删除之
		auto raftNode = this->_raftNode.lock();
		if(raftNode && raftNode->isLeader())
		{
			vector<QueueIndex> req;
			QueueIndex qi;
			qi.queue = queue;
			qi.index = index;
			req.push_back(qi);

			TarsOutputStream<BufferWriterString> os;

			os.write(StorageStateMachine::DELDATA_QUEUE_TYPE, 0);
			os.write(req, 1);

			shared_ptr<ApplyContext> context = std::make_shared<ApplyContext>();
			raftNode->replicate(os.getByteBuffer(), context);
		}
	}
}

int StorageStateMachine::get_queue(const QueuePopReq &req, vector<QueueRsp> &rsp)
{
	TLOG_DEBUG("queue:" << req.queue << endl);

	auto handle = getQueue(req.queue);
	if(!handle)
	{
		TLOG_ERROR("queue:" << req.queue << ", queue not exists!" << endl);
		return S_QUEUE_NOT_EXIST;
	}

	if(req.back)
	{
		auto_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions(), handle));

		it->SeekToLast();

		while(rsp.size() < req.count)
		{
			if (!it->Valid())
			{
				TLOG_DEBUG("queue:" << req.queue << " no data, size:" << rsp.size() << ", count size:" << req.count << endl);

				return S_OK;
			}

			int64_t index = *(int64_t*)(it->key().data());
			rocksdb::Slice value = it->value();

			get_data(req.queue, index, value.data(), value.size(), rsp);

			it->Prev();
		}
	}
	else
	{
		auto_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions(), handle));

		it->SeekToFirst();

		while(rsp.size() < req.count)
		{
			if (!it->Valid())
			{
				TLOG_DEBUG("queue:" << req.queue << " no data, size:" << rsp.size() << ", count size:" << req.count << endl);

				return S_OK;
			}

			int64_t index = *(int64_t*)(it->key().data());

			rocksdb::Slice value = it->value();

			get_data(req.queue, index, value.data(), value.size(), rsp);

			it->Next();
		}

	}

	return S_OK;
}

int StorageStateMachine::getQueueData(const vector<QueueIndex> &req, vector<QueueRsp> &rsp)
{
	for(auto &r : req)
	{
		auto handle = getQueue(r.queue);
		if (!handle)
		{
			TLOG_ERROR("queue:" << r.queue << ", queue not exists!" << endl);
			return S_QUEUE_NOT_EXIST;
		}

		string value;

		auto s = _db->Get(rocksdb::ReadOptions(), handle, rocksdb::Slice((const char*)&r.index, sizeof(r.index)), &value);
		if (s.ok())
		{
			get_data(r.queue, r.index, value.data(), value.size(), rsp);
		}
		else if (s.IsNotFound())
		{
			continue;
		}
		else
		{
			TLOG_ERROR("Get: " << r.queue << ", index: " << r.index << ", error:" << s.ToString() << endl);

			return S_ERROR;
		}
	}
	return S_OK;
}

void StorageStateMachine::onCreateQueue(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	string queue;
	is.read(queue, 1, false);

	queue = queueName(queue);

	TLOG_DEBUG("queue name:" << queue << ", appliedIndex:" << appliedIndex << endl);

	int ret = S_OK;
	rocksdb::WriteBatch batch;

	writeBatch(batch, appliedIndex);

	bool exist = false;

	{
		std::lock_guard<std::mutex> lock(_mutex);
		if(_column_familys.find(queue) != _column_familys.end())
		{
			exist = true;
		}
	}

	TLOG_ERROR("queue name:" << queue << ", exists:" << exist << endl);

	if(exist)
	{
		if(callback)
		{
			//如果客户端请求过来的, 直接回包
			//如果是其他服务器同步过来, 不用回包了
			TLOG_DEBUG("queue name:" << queue << ", appliedIndex:" << appliedIndex << ", response succ S_QUEUE_EXIST" << endl);

			Storage::async_response_createQueue(callback->getCurrentPtr(), S_QUEUE_EXIST);
		}
	}
	else
	{
		rocksdb::ColumnFamilyHandle* handle;
		rocksdb::ColumnFamilyOptions options;
		options.comparator = new QueueKeyComparator();
		options.compaction_filter = nullptr;

		auto status = _db->CreateColumnFamily(options, queue, &handle);
		if (!status.ok())
		{
			TLOG_ERROR("CreateColumnFamily error:" << status.ToString() << endl);
			throw std::runtime_error(status.ToString());
		}

		{
			std::lock_guard<std::mutex> lock(_mutex);
			_column_familys[queue] = handle;
			_queues.push_back(queue.substr(2));
		}
		if(callback)
		{
			TLOG_DEBUG("queue name:" << queue << ", appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

			Storage::async_response_createQueue(callback->getCurrentPtr(), ret);
		}
	}
}

void StorageStateMachine::onDeleteDataQueue(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	vector<QueueIndex> req;

	is.read(req, 1, false);

	TLOG_DEBUG("queue, appliedIndex:" << appliedIndex << ", delete size:" << req.size() << endl);
	rocksdb::WriteBatch batch;

	int ret;

	for(auto &r : req)
	{
		auto handle = getQueue(r.queue);

		if (!handle)
		{
			TLOG_ERROR("queue:" << r.queue << ", queue not exists" << endl);

			ret = S_QUEUE_NOT_EXIST;
			break;
		}
		else
		{
			batch.Delete(handle, rocksdb::Slice((const char*)&r.index, sizeof(r.index)));

			ret = S_OK;
		}
	}

	if(ret != S_OK)
	{
		batch.Clear();
	}

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_deleteQueueData(callback->getCurrentPtr(), ret);
	}
}

STORAGE_RT StorageStateMachine::queueBatch(rocksdb::WriteBatch &batch, const vector<QueuePushReq> &req)
{
	int ret;
	unordered_map<string, int64_t> backIndex;
	unordered_map<string, int64_t> frontIndex;

	for(auto &r : req)
	{
		auto handle = getQueue(r.queue);

		if (!handle)
		{
			TLOG_ERROR("queue:" << r.queue << ", queue not exists" << endl);

			ret = S_QUEUE_NOT_EXIST;
			break;
		}
		else
		{
			int64_t index = 0;

			if(r.back)
			{
				auto it = backIndex.find(r.queue);

				if(it == backIndex.end())
				{
					auto_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions(), handle));

					it->SeekToLast();

					if (it->Valid())
					{
						index = *(int64_t*)(it->key().data()) + 1;
					}

					backIndex[r.queue] = index;
				}
				else
				{
					++it->second;

					index = it->second;
				}

			}
			else
			{
				auto it = frontIndex.find(r.queue);

				if(it == frontIndex.end())
				{
					auto_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions(), handle));

					it->SeekToLast();

					if (it->Valid())
					{
						index = *(int64_t*)(it->key().data()) - 1;
					}

					frontIndex[r.queue] = index;
				}
				else
				{
					--it->second;

					index = it->second;
				}
			}

//			LOG_CONSOLE_DEBUG << index << ", " << string(r.data.data(), r.data.size()) << endl;

			TarsOutputStream<> buff;
			QueueData qd;
			if(r.expireTime > 0)
			{
				qd.expireTime = r.expireTime + TNOW;
			}

			qd.data = std::move(r.data);

			qd.writeTo(buff);

			batch.Put(handle, rocksdb::Slice((const char*)&index, sizeof(index)),
					rocksdb::Slice(buff.getBuffer(), buff.getLength()));

			ret = S_OK;
		}
	}

	if(ret != S_OK)
	{
		batch.Clear();
	}

	return (STORAGE_RT)ret;
}

void StorageStateMachine::onSetDataQueue(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	vector<QueueRsp> data;

	is.read(data, 1, false);
	int ret;

	TLOG_DEBUG("queue, appliedIndex:" << appliedIndex << ", set size:" << data.size() << endl);

	rocksdb::WriteBatch batch;

	for(auto &r : data)
	{
		auto handle = getQueue(r.queue);

		if (!handle)
		{
			TLOG_ERROR("queue:" << r.queue << ", queue not exists" << endl);

			ret = S_QUEUE_NOT_EXIST;
			break;
		}
		else
		{
			TarsOutputStream<> buff;
			QueueData qd;
			if(r.expireTime > 0)
			{
				qd.expireTime = r.expireTime + TNOW;
			}

			qd.data = std::move(r.data);

			qd.writeTo(buff);

			batch.Put(handle, rocksdb::Slice((const char*)&r.index, sizeof(r.index)),
					rocksdb::Slice(buff.getBuffer(), buff.getLength()));

			ret = S_OK;
		}
	}

	if(ret != S_OK)
	{
		batch.Clear();
	}

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_setQueueData(callback->getCurrentPtr(), ret);
	}
}


void StorageStateMachine::onPushQueue(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	vector<QueuePushReq> req;

	is.read(req, 1, false);

	TLOG_DEBUG("queue, appliedIndex:" << appliedIndex << ", push size:" << req.size() << endl);

	int ret;
	rocksdb::WriteBatch batch;

	ret = queueBatch(batch, req);

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_push_queue(callback->getCurrentPtr(), ret);
	}
}

void StorageStateMachine::onPopQueue(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	QueuePopReq req;

	is.read(req, 1, false);

	TLOG_DEBUG("queue, appliedIndex:" << appliedIndex << ", queue:" << req.queue << endl);

	int ret = S_OK;

	rocksdb::WriteBatch batch;

	vector<QueueRsp> rsp;
	auto handle = getQueue(req.queue);
	if(!handle)
	{
		TLOG_ERROR("queue:" << req.queue << ", queue not exists" << endl);

		ret = S_QUEUE_NOT_EXIST;
	}
	else
	{
		auto_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions(), handle));

		if (req.back)
		{
			it->SeekToLast();

			while ((int)rsp.size() < req.count)
			{
				if (!it->Valid())
				{
					TLOG_DEBUG("queue:" << req.queue << " no data, size:" << rsp.size() << ", count size:" << req.count
										<< endl);
					break;
				}

				int64_t index = *(int64_t*)(it->key().data());

				rocksdb::Slice value = it->value();

				get_data(req.queue, index, value.data(), value.size(), rsp);

				batch.Delete(handle, rocksdb::Slice((const char *)&index, sizeof(int64_t)));

				it->Prev();
			}
		}
		else
		{
			it->SeekToFirst();

			while ((int)rsp.size() < req.count)
			{
				if (!it->Valid())
				{
					TLOG_DEBUG("queue:" << req.queue << " no data, size:" << rsp.size() << ", count size:" << req.count
										<< endl);
					break;
				}

				int64_t index = *(int64_t*)(it->key().data());
				rocksdb::Slice value = it->value();

				get_data(req.queue, index, value.data(), value.size(), rsp);

				batch.Delete(handle, rocksdb::Slice((const char *)&index, sizeof(int64_t)));

				it->Next();
			}
		}
	}

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("queue:" << req.queue << " succ, buff size:" << rsp.size() << endl);
		Storage::async_response_pop_queue(callback->getCurrentPtr(), ret, rsp);
	}
}

void StorageStateMachine::onBatch(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	BatchDataReq req;
	is.read(req, 1, false);

	rocksdb::WriteBatch batch;

	int ret;
	BatchDataRsp rsp;

	ret = setBatch(batch, req.sData, rsp.sRsp);

	if(ret == S_OK)
	{
		ret = updateBatch(batch, req.uData);
	}

	if(ret == S_OK)
	{
		ret = queueBatch(batch, req.qData);
	}

	writeBatch(batch, appliedIndex);

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_doBatch(callback->getCurrentPtr(), ret, rsp);
	}
}

int StorageStateMachine::listTable(vector<string> &tables)
{
	std::lock_guard<std::mutex> lock(_mutex);

	tables = _tables;

	return S_OK;
}

int StorageStateMachine::listQueue(vector<string> &queues)
{
	std::lock_guard<std::mutex> lock(_mutex);

	queues = _queues;

	return S_OK;
}

int StorageStateMachine::transQueue(const QueuePageReq &req, vector<QueueRsp> &data)
{
	data.clear();

	auto handle = getQueue(req.queue);
	if(!handle)
	{
		TLOG_ERROR("queue:" << req.queue << ", index:" << req.index << ", queue not exists" << endl);

		return S_QUEUE_NOT_EXIST;
	}

	auto_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions(), handle));

	if(req.forward)
	{
		shared_ptr<AutoSlice> k;
		int64_t *p = new int64_t;

		if(req.index.empty())
		{
			*p = std::numeric_limits<int64_t>::min();
			k = std::make_shared<AutoSlice>((const char *)p, sizeof(int64_t));
		}
		else
		{
			*p = TC_Common::strto<int64_t>(req.index);

			k = std::make_shared<AutoSlice>((const char *)p, sizeof(int64_t));
		}

		rocksdb::Slice key(k->data, k->length);

		//从小到大
		it->Seek(key);
		while(it->Valid() && (req.limit < 0 || data.size() < req.limit))
		{
			int64_t index = *(int64_t*)it->key().data();

			if(!req.include && *p == index)
			{
				//不包含检索的第一个数据
				it->Next();
				continue;
			}

			get_data(req.queue, index, it->value().data(), it->value().size(), data);

			it->Next();
		}

	}
	else
	{
		shared_ptr<AutoSlice> k;
		int64_t *p = new int64_t;
		if(req.index.empty())
		{
			*p = std::numeric_limits<int64_t>::max();
			k = std::make_shared<AutoSlice>((const char *)p, sizeof(int64_t));
		}
		else
		{
			*p = TC_Common::strto<int64_t>(req.index);

			k = std::make_shared<AutoSlice>((const char *)p, sizeof(int64_t));
		}

		//从大到小
		it->SeekForPrev(rocksdb::Slice(k->data, k->length));

		while(it->Valid() && (req.limit < 0 || data.size() < req.limit))
		{
			int64_t index = *(int64_t*)it->key().data();

			if(!req.include && *p == index)
			{
				//不包含检索的第一个数据
				it->Prev();
				continue;
			}

			get_data(req.queue, index, it->value().data(), it->value().size(), data);

			it->Prev();
		}
	}

	TLOG_DEBUG("queue:" << req.queue << ", index:" << req.index << ", forward:" << req.forward << ", limit:" << req.limit << ", result size: " << data.size() << endl);

	return S_OK;
}


void StorageStateMachine::onDeleteQueue(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	string queue;
	is.read(queue, 1, false);

	TLOG_DEBUG("queue:" << queue << endl);

	auto handle = getQueue(queue);

	if(handle)
	{
		{
			std::lock_guard<std::mutex> lock(_mutex);

			_column_familys.erase(handle->GetName());

			_queues.erase(std::remove(_queues.begin(), _queues.end(), handle->GetName().substr(2)), _queues.end());

		}

		_db->DropColumnFamily(handle);

		_db->DestroyColumnFamilyHandle(handle);
	}

	if(callback)
	{
		Storage::async_response_deleteQueue(callback->getCurrentPtr(), S_OK);
	}
}
