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

		os.write(StorageStateMachine::QUEUE_TYPE, 0);
		os.write(queue, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::push_back(const QueueReq &req, CurrentPtr current)
{
	if(req.queue.empty())
	{
		return S_QUEUE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::PUSH_BACK_TYPE, 0);
		os.write(req, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::push_front(const QueueReq &req, CurrentPtr current)
{
	if(req.queue.empty())
	{
		return S_QUEUE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::PUSH_FRONT_TYPE, 0);
		os.write(req, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::get_front(const Options &options, const string &queue, QueueRsp &rsp, CurrentPtr current)
{
	if(queue.empty())
	{
		return S_QUEUE_NAME;
	}
	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->get_front(queue, rsp);
}

int StorageImp::get_back(const Options &options, const string &queue, QueueRsp &rsp, CurrentPtr current)
{
	if(queue.empty())
	{
		return S_QUEUE_NAME;
	}
	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->get_back(queue, rsp);
}

int StorageImp::pop_front(const string &queue, QueueRsp &rsp, CurrentPtr current)
{
	if(queue.empty())
	{
		return S_QUEUE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::POP_FRONT_DEL_TYPE, 0);
		os.write(queue, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::pop_back(const string &queue, QueueRsp &rsp, CurrentPtr current)
{
	if(queue.empty())
	{
		return S_QUEUE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::POP_BACK_DEL_TYPE, 0);
		os.write(queue, 1);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::deleteData(const string &queue, tars::Int64 index, CurrentPtr current)
{
	if(queue.empty())
	{
		return S_QUEUE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::DEL_DATA_TYPE, 0);
		os.write(queue, 1);
		os.write(index, 2);

		return  os.getByteBuffer();
	});

	return 0;
}

int StorageImp::getData(const Options &options, const string &queue, tars::Int64 index, QueueRsp &rsp, CurrentPtr current)
{
	if(queue.empty())
	{
		return S_QUEUE_NAME;
	}
	if(options.leader && !_raftNode->isLeader())
	{
		_raftNode->forwardToLeader(current);
		return 0;
	}
	return _stateMachine->get(queue, index, rsp);
}

int StorageImp::clearQueue(const string &queue, CurrentPtr current)
{
	if(queue.empty())
	{
		return S_QUEUE_NAME;
	}
	_raftNode->forwardOrReplicate(current, [&](){

		TarsOutputStream<BufferWriterString> os;

		os.write(StorageStateMachine::CLEAR_QUEUE_TYPE, 0);
		os.write(queue, 1);

		return  os.getByteBuffer();
	});

	return 0;
}