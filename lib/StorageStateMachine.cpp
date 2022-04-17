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
using namespace Base;

const string StorageStateMachine::SET_TYPE  = "1";
const string StorageStateMachine::BSET_TYPE = "2";
const string StorageStateMachine::DEL_TYPE  = "3";
const string StorageStateMachine::BDEL_TYPE = "4";
const string StorageStateMachine::TABLE_TYPE = "5";
const string StorageStateMachine::SET_JSON_TYPE = "6";
const string StorageStateMachine::BSET_JSON_TYPE = "7";

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

		int flag = TC_Port::strcmp(a.data() + sizeof(uint32_t), b.data() + sizeof(uint32_t));

		if(flag != 0)
		{
			return flag;
		}

		if(a[sizeof(uint32_t) + amkeyLen + 1] != b[sizeof(uint32_t) + bmkeyLen + 1])
		{
			return (unsigned char)(a[sizeof(uint32_t) + amkeyLen + 1]) < (unsigned char)(b[sizeof(uint32_t) + bmkeyLen + 1]) ? -1 : 1;
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
StorageStateMachine::StorageStateMachine(const string &dataPath)
{
	_raftDataDir = dataPath;

	_onApply[SET_TYPE] = std::bind(&StorageStateMachine::onSet, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[BSET_TYPE] = std::bind(&StorageStateMachine::onSetBatch, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[DEL_TYPE] = std::bind(&StorageStateMachine::onDel, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[BDEL_TYPE] = std::bind(&StorageStateMachine::onDelBatch, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[TABLE_TYPE] = std::bind(&StorageStateMachine::onCreateTable, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[SET_JSON_TYPE] = std::bind(&StorageStateMachine::onUpdate, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);
	_onApply[BSET_JSON_TYPE] = std::bind(&StorageStateMachine::onUpdateBatch, this, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

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

rocksdb::ColumnFamilyHandle* StorageStateMachine::get(const string &table)
{
	//防止和系统的default重名!
	string dTable = tableName(table);//"d-" + table;

	std::lock_guard<std::mutex> lock(_mutex);

	auto it = _column_familys.find(dTable);
	if(it != _column_familys.end())
	{
		return it->second;
	}

	TLOG_ERROR("no table:" << dTable << endl);
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
	options.compaction_filter = new TTLCompactionFilter();

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
				c.options.comparator = new StorageKeyComparator();
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
		exit(-1);
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
	assert(it != _onApply.end());

	it->second(is, appliedIndex, callback);
}

int StorageStateMachine::has(const StorageKey &skey)
{
	auto handle = get(skey.table);
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

		return S_ERROR;
	}

	return S_OK;
}

bool StorageStateMachine::isExpire(TarsInputStream<> &is)
{
	int expireTime = 0;
	is.read(expireTime, 0, false);

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

	auto handle = get(skey.table);
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

		auto handle = get(skey[i].table);
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

		auto handle = get(skey[i].table);
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

	auto handle = get(req.skey.table);
	if(!handle)
	{
		TLOG_ERROR("table:" << req.skey.table << ", mkey:" << req.skey.mkey << ", ukey:" << req.skey.ukey << ", table not exists" << endl);

		return S_TABLE_NOT_EXIST;
	}

	auto_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions(), handle));

	if(req.forword)
	{
		auto k = tokey(req.skey);

		rocksdb::Slice key(k->data, k->length);

		//从小到大
		it->Seek(key);
		while(it->Valid() && (req.limit < 0 || data.size() < req.limit))
		{
			StorageData d;
			d.skey = keyto(it->key().data(), it->key().size());

			if(d.skey.mkey != req.skey.mkey)
			{
				//跨mkey了
				break;
			}

			if(!req.include && req.skey.ukey == d.skey.ukey)
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

		if(!req.skey.ukey.empty())
		{
			k = tokey(req.skey);
		}
		else
		{
			k = tokeyUpper(req.skey.mkey);
		}

		//从大到小
		it->SeekForPrev(rocksdb::Slice(k->data, k->length));

		while(it->Valid() && (req.limit < 0 || data.size() < req.limit))
		{
			StorageData d;
			d.skey = keyto(it->key().data(), it->key().size());

			if(d.skey.mkey != req.skey.mkey)
			{
				//跨mkey了
				break;
			}

			if(!req.include && req.skey.ukey == d.skey.ukey)
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

	TLOG_ERROR("table:" << req.skey.table << ", mkey:" << req.skey.mkey << ", ukey:" << req.skey.ukey << ", result size: " << data.size() << endl);

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
		exit(-1);
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

	auto handle = get(data.skey.table);

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

			rocksdb::WriteBatch batch;

			batch.Put(handle, rocksdb::Slice(key->data, key->length), rocksdb::Slice(buff.getBuffer(), buff.getLength()));

			writeBatch(batch, appliedIndex);
		}
	}

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_set(callback->getCurrentPtr(), ret);
	}
}

int StorageStateMachine::onUpdateJson(rocksdb::WriteBatch &batch, const StorageJson &update)
{
	STORAGE_RT ret = S_OK;

	auto handle = get(update.skey.table);

	if(!handle)
	{
		TLOG_ERROR("table:" << update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey << ", table not exists" << endl);

		ret = S_TABLE_NOT_EXIST;
	}
	else
	{
		std::string value;
		auto key = tokey(update.skey);
		rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), handle, rocksdb::Slice(key->data, key->length), &value);
		if (s.ok() || s.IsNotFound())
		{
			ret = S_OK;

			StorageValue data;

			JsonValueObjPtr json;

			if(s.ok())
			{
				TarsInputStream<> is;
				is.setBuffer(value.c_str(), value.length());
				data.readFrom(is);

				json = JsonValueObjPtr::dynamicCast(TC_Json::getValue(data.data));

				if (!json)
				{
					TLOG_ERROR(update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey << ", parse to json error." << endl);

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

			if(ret != S_OK)
			{
				return ret;
			}

			for(auto &e : update.supdate)
			{
				auto it = json->value.find(e.field);
				if(it != json->value.end())
				{
					auto tIt = this->_updateApply.find(it->second->getType());
					if(tIt != this->_updateApply.end())
					{
						auto rIt = tIt->second.find(e.op);

						if(rIt != tIt->second.end())
						{
							ret = rIt->second(it->second, e);

							if(ret != S_OK)
							{
								TLOG_ERROR(update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey << ", field:" << e.field << " , op:" << etos(e.op) << ", ret:" << etos(ret) << endl);
								break;
							}
						}
						else
						{
							ret = Base::S_JSON_OPERATOR_NOT_SUPPORT;

							TLOG_ERROR(update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey << ", field:" << e.field << " , op:" << etos(e.op) << ", ret:" << etos(ret) << endl);

							break;
						}
					}
				}
				else if(!e.def.empty())
				{
					JsonValuePtr value;

					switch(e.type)
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
						value = new JsonValueBoolean(TC_Port::strncasecmp(e.def.c_str(),"true", e.def.size()) == 0);
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

					if(ret != S_OK)
					{
						break;
					}

					if(value)
					{
						json->value[e.field] = value;
					}
				}
				else
				{
					ret = S_JSON_FIELD_NOT_EXITS;
					TLOG_ERROR(update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey << ", field:" << e.field << " in not exists, op:" << etos(e.op) << ", ret:" << etos(ret) << endl);
					break;
				}
			}

			if(ret != S_OK)
			{
				return ret;
			}

			TC_Json::writeValue(json, data.data);

			if (data.expireTime > 0)
			{
				data.expireTime += TNOW;
			}

			TarsOutputStream<> buff;
			while (data.version == 0)
			{
				++data.version;
			}

			data.writeTo(buff);

			batch.Put(handle, rocksdb::Slice(key->data, key->length), rocksdb::Slice(buff.getBuffer(), buff.getLength()));
		}
		else
		{
			TLOG_ERROR("Get: " << update.skey.mkey + "-" + update.skey.ukey << ", error:" << s.ToString() << endl);

			ret = S_ERROR;
		}
	}

	return ret;
}
void StorageStateMachine::onUpdate(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	StorageJson update;

	is.read(update, 1, false);

	TLOG_DEBUG("table:" << update.skey.table << ", mkey:" << update.skey.mkey << ", ukey:" << update.skey.ukey
						<< ", appliedIndex:" << appliedIndex << ", update:" << TC_Common::tostr(update.supdate.begin(), update.supdate.end(), " ") << endl);

	rocksdb::WriteBatch batch;

	int ret = onUpdateJson(batch, update);

	if(ret == S_OK)
	{
		writeBatch(batch, appliedIndex);
	}

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_update(callback->getCurrentPtr(), ret);
	}
}


void StorageStateMachine::onUpdateBatch(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	vector<StorageJson> updates;

	is.read(updates, 1, false);

	rocksdb::WriteBatch batch;

	STORAGE_RT ret = S_OK;

	for(auto &update : updates)
	{
		int ret = onUpdateJson(batch, update);

		if(ret != S_OK)
		{
			break;
		}
	}

	if(ret == S_OK)
	{
		writeBatch(batch, appliedIndex);
	}

	if(callback)
	{
		TLOG_DEBUG("appliedIndex:" << appliedIndex << ", response ret:" << etos((STORAGE_RT)ret) << endl);

		Storage::async_response_update(callback->getCurrentPtr(), ret);
	}
}

void StorageStateMachine::onSetBatch(TarsInputStream<> &is, int64_t appliedIndex, const shared_ptr<ApplyContext> &callback)
{
	vector<StorageData> vdata;

	is.read(vdata, 1, false);

	TLOG_DEBUG("appliedIndex:" << appliedIndex << ", data size:" << vdata.size() << endl);

	map<StorageKey, int> rsp;

	int bret = S_OK;

	rocksdb::WriteBatch batch;

	for(auto &data : vdata)
	{
		auto handle = get(data.skey.table);

		if(!handle)
		{
			bret = S_ERROR;
			rsp[data.skey] = S_TABLE_NOT_EXIST;
		}
		else
		{
			int ret = checkStorageData(handle, data);

			if(ret != S_OK)
			{
				bret = S_ERROR;
				rsp[data.skey] = ret;
			}
			else
			{
				if(data.svalue.expireTime>0) {
					data.svalue.expireTime += TNOW;
				}

				TarsOutputStream<> buff;
				while(data.svalue.version == 0)
				{
					++data.svalue.version;
				}

				data.svalue.writeTo(buff);

				auto key = tokey(data.skey);
				batch.Put(handle, rocksdb::Slice(key->data, key->length), rocksdb::Slice(buff.getBuffer(), buff.getLength()));

				rsp[data.skey] = S_OK;
			}
		}
	}

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
		batch.Delete(get(skey.table), rocksdb::Slice(key->data, key->length));
	}
	else
	{
		auto upperKey = tokeyUpper(skey.mkey);
		batch.DeleteRange(get(skey.table), rocksdb::Slice(key->data, key->length), rocksdb::Slice(upperKey->data, upperKey->length));
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
		auto handle = get(skey.table);
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
			batch.DeleteRange(get(skey.table), rocksdb::Slice(key->data, key->length), rocksdb::Slice(upperKey->data, upperKey->length));
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

		auto status = _db->CreateColumnFamily(options, table, &handle);
		if (!status.ok())
		{
			TLOG_ERROR("CreateColumnFamily error:" << status.ToString() << endl);
			throw std::runtime_error(status.ToString());
		}

		{
			std::lock_guard<std::mutex> lock(_mutex);
			_column_familys[table] = handle;
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