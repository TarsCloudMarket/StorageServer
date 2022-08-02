//
// Created by jarod on 2019-07-22.
//

#include "StorageImp.h"
#include "StorageServer.h"
//#include "RaftNode.h"
#include "StorageStateMachine.h"

extern StorageServer g_app;

void StorageImp::initialize()
{
	_raftNode = ((StorageServer*)this->getApplication())->node() ;
	_stateMachine = ((StorageServer*)this->getApplication())->getStateMachine();
}

void StorageImp::destroy()
{

}

int StorageImp::createTable(const string &table, CurrentPtr current)
{
	if(table.empty())
	{
		return S_TABLE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::TABLE_TYPE, 0);
		os.write(table, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::has(const Options &options, const StorageKey &skey, CurrentPtr current)
{
	if(skey.table.empty())
	{
		return S_TABLE_NAME;
	}
	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->has(skey);
}

int StorageImp::get(const Options &options, const StorageKey &skey, StorageValue &data, CurrentPtr current)
{
	if(skey.table.empty())
	{
		return S_TABLE_NAME;
	}
	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->get(skey, data);
}

int StorageImp::set(const StorageData &data, CurrentPtr current)
{
	if(data.skey.table.empty())
	{
		return S_TABLE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::SET_TYPE, 0);
		os.write(data, 1);	

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::update(const StorageJson &data, CurrentPtr current)
{
	if(data.skey.table.empty())
	{
		return S_TABLE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::SET_JSON_TYPE, 0);
		os.write(data, 1);

		return  os.getByteBuffer();
	});

	return 0;
}
int StorageImp::del(const StorageKey &skey, CurrentPtr current)
{
	if(skey.table.empty())
	{
		return S_TABLE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::DEL_TYPE, 0);
		os.write(skey, 1);

		return  os.getByteBuffer();
	});

	return  0;
}

int StorageImp::hasBatch(const Options &options, const vector<StorageKey> &data, map<StorageKey, int> &rsp, CurrentPtr current)
{
	for(auto &d : data)
	{
		if(d.table.empty())
		{
			return S_TABLE_NAME;
		}
	}

	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->hasBatch(data, rsp);
}

int StorageImp::getBatch(const Options &options, const vector<StorageKey> &skey, vector<StorageData> &data, CurrentPtr current)
{
	for(auto &k : skey)
	{
		if(k.table.empty())
		{
			return S_TABLE_NAME;
		}
	}

	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->get(skey, data);
}

int StorageImp::setBatch(const vector<StorageData> &data, map<StorageKey, int> &rsp, CurrentPtr current)
{
	for(auto &d : data)
	{
		if(d.skey.table.empty())
		{
			return S_TABLE_NAME;
		}
	}

	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::BSET_TYPE, 0);
		os.write(data, 1);	

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::updateBatch(const vector<StorageJson> &data, CurrentPtr current)
{
	for(auto &k : data)
	{
		if(k.skey.table.empty())
		{
			return S_TABLE_NAME;
		}
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::BSET_JSON_TYPE, 0);
		os.write(data, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::delBatch(const vector<StorageKey> &skey, CurrentPtr current)
{
	for(auto &k : skey)
	{
		if(k.table.empty())
		{
			return S_TABLE_NAME;
		}
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::BDEL_TYPE, 0);
		os.write(skey, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::trans(const Options &options, const PageReq &req, vector<StorageData> &data, CurrentPtr current)
{
	if(req.skey.table.empty())
	{
		return S_TABLE_NAME;
	}
	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->trans(req, data);
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////

int StorageImp::createQueue(const string &queue, CurrentPtr current)
{
	if(queue.empty())
	{
		return S_QUEUE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::CREATE_QUEUE_TYPE, 0);
		os.write(queue, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::push_queue(const vector<QueuePushReq> &req, CurrentPtr current)
{
	for(auto &r : req)
	{
		if (r.queue.empty())
		{
			return S_QUEUE_NAME;
		}
	}

	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::PUSH_QUEUE_TYPE, 0);
		os.write(req, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::get_queue(const Options &options, const QueuePopReq &req, vector<QueueRsp> &rsp, CurrentPtr current)
{
	if(req.queue.empty())
	{
		return S_QUEUE_NAME;
	}
	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->get_queue(req, rsp);
}


int StorageImp::pop_queue(const QueuePopReq &req, vector<QueueRsp> &rsp, CurrentPtr current)
{
	if (req.queue.empty())
	{
		return S_QUEUE_NAME;
	}

	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::POP_QUEUE_TYPE, 0);
		os.write(req, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::deleteQueueData(const vector<QueueIndex> &req, CurrentPtr current)
{
	for(auto &r : req)
	{
		if (r.queue.empty())
		{
			return S_QUEUE_NAME;
		}
	}

	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::DEL_QUEUE_TYPE, 0);
		os.write(req, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::getQueueData(const Options &options, const vector<QueueIndex> &req, vector<QueueRsp> &rsp, CurrentPtr current)
{
	for(auto &r : req)
	{
		if (r.queue.empty())
		{
			return S_QUEUE_NAME;
		}
	}

	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->getQueueData(req, rsp);
}

int StorageImp::getQueueSize(const Options &options, const string &queue, tars::Int64  &size, CurrentPtr current)
{
	if (queue.empty())
	{
		return S_QUEUE_NAME;
	}
	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->getQueueSize(queue, size);
}

int StorageImp::doBatch(const BatchDataReq &req, BatchDataRsp &rsp, CurrentPtr current)
{
	for(auto &d : req.sData)
	{
		if(d.skey.table.empty())
		{
			return S_TABLE_NAME;
		}
	}

	for(auto &d : req.uData)
	{
		if(d.skey.table.empty())
		{
			return S_TABLE_NAME;
		}
	}

	for(auto &d : req.qData)
	{
		if(d.queue.empty())
		{
			return S_QUEUE_NAME;
		}
	}

	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::BATCH_DATA, 0);
		os.write(req, 1);

		return  os.getByteBuffer();
	});

	return 0;
}