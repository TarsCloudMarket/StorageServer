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
	string dataPath = ServerConfig::DataPath;

	if(!TC_Port::getEnv("KUBERNETES_PORT").empty())
	{
		dataPath = "/storage-data/";
	}

	//for debug
	if(ServerConfig::DataPath == "./debug-data/")
	{
		_index = TC_Endpoint(getConfig().get("/tars/application/server/Base.StorageServer.RaftObjAdapter<endpoint>", "")).getPort();

		_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 127.0.0.1 -p 10101"), TC_Endpoint("tcp -h 127.0.0.1 -p 10401 -t 60000")));
		_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 127.0.0.1 -p 10102"), TC_Endpoint("tcp -h 127.0.0.1 -p 10402 -t 60000")));
		_nodeInfo.nodes.push_back(std::make_pair(TC_Endpoint("tcp -h 127.0.0.1 -p 10103"), TC_Endpoint("tcp -h 127.0.0.1 -p 10403 -t 60000")));
	}

	LOG_CONSOLE_DEBUG << "data path:" << ServerConfig::DataPath << ", index:" << _index << ", node size:" << _nodeInfo.nodes.size() << endl;

	RaftOptions raftOptions;
	raftOptions.electionTimeoutMilliseconds = 1000;
	raftOptions.heartbeatPeriodMilliseconds = 300;

	raftOptions.dataDir                     = dataPath + "raft-log-" + TC_Common::tostr(_index);
	raftOptions.snapshotPeriodSeconds       = 600;
	raftOptions.maxLogEntriesPerRequest     = 100;
	raftOptions.maxLogEntriesMemQueue       = 3000;
	raftOptions.maxLogEntriesTransfering    = 1000;

	onInitializeRaft(raftOptions, "StorageObj", dataPath + "StorageLog-" + TC_Common::tostr(_index));
}

void StorageServer::destroyApp()
{
	_stateMachine->close();

	onDestroyRaft();
}