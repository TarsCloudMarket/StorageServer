//
// Created by jarod on 2019-08-08.
//

#include "StorageServer.h"
#include "StorageImp.h"
#include "StorageStateMachine.h"
#include "RaftImp.h"
#include "RaftOptions.h"

void StorageServer::initialize()
{
	string dataPath;
	TC_Config conf;

//	LOG_CONSOLE_DEBUG << "data path:" << ServerConfig::DataPath << ", nodes size:" << _nodeInfo.nodes.size() << endl;

	//for debug
	if(ServerConfig::DataPath == "./debug-data/")
	{
		dataPath = ServerConfig::DataPath;

		_index = TC_Endpoint(getConfig().get("/tars/application/server/Base.StorageServer.RaftObjAdapter<endpoint>", "")).getPort();

		if(_nodeInfo.nodes.empty())
		{
			//_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 172.30.0.3 -p 10101"), TC_Endpoint("tcp -h 172.30.0.3 -p 10401 -t 60000")));
			_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 127.0.0.1 -p 10101"),
					TC_Endpoint("tcp -h 127.0.0.1 -p 10401 -t 60000")));
//		_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 172.30.0.33 -p 10102"), TC_Endpoint("tcp -h 172.30.0.33 -p 10402 -t 60000")));
			_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 127.0.0.1 -p 10102"),
					TC_Endpoint("tcp -h 127.0.0.1 -p 10402 -t 60000")));
//		_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 172.30.0.34 -p 10103"), TC_Endpoint("tcp -h 172.30.0.34 -p 10403 -t 60000")));
			_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 127.0.0.1 -p 10103"),
					TC_Endpoint("tcp -h 127.0.0.1 -p 10403 -t 60000")));
		}
	}
	else
	{
		addConfig("storage.conf");
		conf.parseFile(ServerConfig::BasePath + "storage.conf");

		dataPath = conf.get("/root<storage-path>");
	}

//	LOG_CONSOLE_DEBUG << "data path:" << ServerConfig::DataPath << ", index:" << _index << ", node size:" << _nodeInfo.nodes.size() << endl;

	RaftOptions raftOptions;
	raftOptions.electionTimeoutMilliseconds = TC_Common::strto<int>(conf.get("/root/raft<electionTimeoutMilliseconds>", "3000"));
	raftOptions.heartbeatPeriodMilliseconds = TC_Common::strto<int>(conf.get("/root/raft<heartbeatPeriodMilliseconds>", "300"));
	raftOptions.snapshotPeriodSeconds       = TC_Common::strto<int>(conf.get("/root/raft<snapshotPeriodSeconds>", "600"));
	raftOptions.maxLogEntriesPerRequest     = TC_Common::strto<int>(conf.get("/root/raft<maxLogEntriesPerRequest>", "100"));
	raftOptions.maxLogEntriesMemQueue       = TC_Common::strto<int>(conf.get("/root/raft<maxLogEntriesMemQueue>", "3000"));
	raftOptions.maxLogEntriesTransfering    = TC_Common::strto<int>(conf.get("/root/raft<maxLogEntriesTransfering>", "1000"));
	raftOptions.dataDir                     = TC_File::simplifyDirectory(dataPath + FILE_SEP + "raft-log-" + TC_Common::tostr(_index));

	TLOG_DEBUG("electionTimeoutMilliseconds:" << raftOptions.electionTimeoutMilliseconds << endl);
	TLOG_DEBUG("heartbeatPeriodMilliseconds:" << raftOptions.heartbeatPeriodMilliseconds << endl);
	TLOG_DEBUG("snapshotPeriodSeconds:" << raftOptions.snapshotPeriodSeconds << endl);
	TLOG_DEBUG("maxLogEntriesPerRequest:" << raftOptions.maxLogEntriesPerRequest << endl);
	TLOG_DEBUG("maxLogEntriesMemQueue:" << raftOptions.maxLogEntriesMemQueue << endl);
	TLOG_DEBUG("maxLogEntriesTransfering:" << raftOptions.maxLogEntriesTransfering << endl);
	TLOG_DEBUG("dataDir:" << raftOptions.dataDir << endl);

	onInitializeRaft(raftOptions, "StorageObj", TC_File::simplifyDirectory(dataPath + FILE_SEP + "StorageLog-" + TC_Common::tostr(_index)));

	TARS_ADD_ADMIN_CMD_NORMAL("storage.get", StorageServer::cmdGet);
	TARS_ADD_ADMIN_CMD_NORMAL("storage.set", StorageServer::cmdSet);
}

void StorageServer::destroyApp()
{
	_stateMachine->close();

	onDestroyRaft();
}


//storage.get table mkey ukey
bool StorageServer::cmdGet(const string&command, const string&params, string& result)
{
	vector<string> v = TC_Common::sepstr<string>(params, " ");

	if(v.size() >= 3)
	{
		StorageKey skey;
		skey.table = v[0];
		skey.mkey = v[1];
		skey.ukey = v[2];

		StorageValue data;
		int ret = _stateMachine->get(skey, data);

		if(ret == S_OK)
		{
			result = "version: " + TC_Common::tostr(data.version) + "\n";
			result += "timestamp: " + TC_Common::tostr(data.timestamp) + "\n";
			result += "expireTime: " + TC_Common::tostr(data.expireTime) + "\n";
			result += "data: " + string(data.data.data(), data.data.size()) + "\n";
		}
		else if(ret == S_NO_DATA)
		{
			result = "no data";
		}
		else
		{
			result = "error, ret:" + etos((STORAGE_RT)ret);
		}
	}
	else
	{
		result = "Invalid parameters.Should be: storage.get table mkey ukey";
	}
	return true;

}

bool StorageServer::cmdSet(const string&command, const string&params, string& result)
{
	vector<string> v = TC_Common::sepstr<string>(params, " ");

	if(v.size() >= 4)
	{
		StorageData data;
		data.skey.table = v[0];
		data.skey.mkey = v[1];
		data.skey.ukey = v[2];
		data.svalue.data.assign(v[3].data(), v[3].data()+v[3].size());

		StoragePrx prx = _raftNode->getBussLeaderPrx<StoragePrx>();
		int ret = prx->set(data);

		if(ret == S_OK)
		{
			result = "set succ";
		}
		else
		{
			result = "set error, ret:" + etos((STORAGE_RT)ret);
		}
	}
	else
	{
		result = "Invalid parameters.Should be: storage.set table mkey ukey value";
	}
	return true;
}