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

//		_index = TC_Endpoint(getConfig().get("/tars/application/server/Base.StorageServer.RaftObjAdapter<endpoint>", "")).getPort();
//
//		if(_nodeInfo.nodes.empty())
//		{
//			_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 127.0.0.1 -p 10101"),
//					TC_Endpoint("tcp -h 127.0.0.1 -p 10401 -t 60000")));
//			_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 127.0.0.1 -p 10102"),
//					TC_Endpoint("tcp -h 127.0.0.1 -p 10402 -t 60000")));
//			_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 127.0.0.1 -p 10103"),
//					TC_Endpoint("tcp -h 127.0.0.1 -p 10403 -t 60000")));
//		}
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
	TARS_ADD_ADMIN_CMD_NORMAL("storage.trans", StorageServer::cmdTrans);
	TARS_ADD_ADMIN_CMD_NORMAL("storage.del", StorageServer::cmdDel);
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

    if(v.size() >= 2)
    {
		StorageKey skey;

        if(v.size() >= 2)
        {
            skey.table = v[0];
            skey.mkey = v[1];
        }

        if(v.size() >= 3)
        {
            skey.ukey = v[2];
        }

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
		result = "Invalid parameters.Should be: storage.get table mkey ukey or storage.get table mkey";
	}
	return true;

}

bool StorageServer::cmdSet(const string&command, const string&params, string& result)
{
	vector<string> v = TC_Common::sepstr<string>(params, " ");

	if(v.size() >= 2)
	{
		StorageData data;

        if(v.size() >= 4)
        {
            data.skey.table = v[0];
            data.skey.mkey = v[1];
            data.skey.ukey = v[2];
            data.svalue.data.assign(v[3].data(), v[3].data()+v[3].size());
        }
        else if(v.size() >= 3)
        {
            data.skey.table = v[0];
            data.skey.mkey = v[1];
            data.svalue.data.assign(v[2].data(), v[2].data()+v[2].size());
        }
		else
		{
			data.skey.table = v[0];
			data.skey.mkey = v[1];
		}

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
		result = "Invalid parameters.Should be: storage.set table mkey ukey value or storage.set table mkey value (ukey is empty) or storage.set table mkey (ukey and value is empty)";
	}
	return true;
}

bool StorageServer::cmdTrans(const string&command, const string&params, string& result)
{
	vector<string> v = TC_Common::sepstr<string>(params, " ");

	if(v.size() >= 3)
	{
		PageReq req;

        if(v.size() >= 5)
        {
            req.skey.table = v[0];
            req.skey.mkey = v[1];
            req.skey.ukey = v[2];
			req.forward = v[3] == "true";
			req.limit = TC_Common::strto<int>(v[4]);
        }
        else 
        {
            req.skey.table = v[0];
            req.skey.mkey = v[1];
			req.forward = v[2] == "true";
			req.limit = TC_Common::strto<int>(v[2]);
        }

		StoragePrx prx = _raftNode->getBussLeaderPrx<StoragePrx>();

		vector<Base::StorageData> datas;
		int ret = _stateMachine->trans(req, datas);

		if(ret == S_OK)
		{
			result = "trans succ, size:" + TC_Common::tostr(datas.size());

			for(size_t i = 0; i< datas.size(); i++)
			{
				result += "------------------------" + TC_Common::tostr(i)+ "--------------------------------\n";
				result += "version: " + TC_Common::tostr(datas[i].svalue.version) + "\n";
				result += "timestamp: " + TC_Common::tostr(datas[i].svalue.timestamp) + "\n";
				result += "expireTime: " + TC_Common::tostr(datas[i].svalue.expireTime) + "\n";
				result += "data: " + string(datas[i].svalue.data.data(), datas[i].svalue.data.size()) + "\n";
			}
		}
		else
		{
			result = "trans error, ret:" + etos((STORAGE_RT)ret);
		}
	}
	else
	{
		result = "Invalid parameters. Should be: storage.trans table mkey ukey forward(true/false) limit or storage.trans table mkey forward(true/false) limit (ukey is empty)";
	}
	return true;

}

bool StorageServer::cmdDel(const string&command, const string&params, string& result)
{
	vector<string> v = TC_Common::sepstr<string>(params, " ");

	if(v.size() >= 3)
	{
		StorageKey skey;

		if(v.size() >= 2)
		{
			skey.table = v[0];
			skey.mkey = v[1];
		}

		if(v.size() >= 3)
		{
			skey.ukey = v[2];
		}

		StoragePrx prx = _raftNode->getBussLeaderPrx<StoragePrx>();

		int ret = prx->del(skey);

		if(ret == S_OK)
		{
			result = "delete succ";
		}
		else
		{
			result = "del error, ret:" + etos((STORAGE_RT)ret);
		}
	}
	else
	{
		result = "Invalid parameters. Should be: storage.del table mkey ukey or storage.del table mkey (ukey is empty means delete all ukey)";
	}
	return true;
}
