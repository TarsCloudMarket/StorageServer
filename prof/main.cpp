#include "Storage.h"
#include <iostream>
#include "servant/Application.h"
#include "util/tc_option.h"

using namespace tars;
using namespace Base;

Communicator *_comm;
//string matchObj = "Base.StorageServer.StorageObj@tcp -h 127.0.0.1 -p 10401:tcp -h 127.0.0.1 -p 10402:tcp -h 127.0.0.1 -p 10403";
string matchObj = "Base.StorageServer.StorageObj@tcp -h 159.75.141.109 -p 8080";
//string matchObj = "Base.StorageServer.RaftObj@tcp -h 159.75.141.109 -p 8080";

struct Param
{
	int count;
	string call;
	int thread;

	StoragePrx pPrx;
};

Param param;
std::atomic<int> callback(0);

struct StorageCallback : public StoragePrxCallback
{
	StorageCallback(int64_t t, int i, int c) : start(t), cur(i), count(c)
	{

	}

	//call back
	virtual void callback_get(tars::Int32 ret,  const Base::StorageValue& rsp)
	{
		++callback;

		if(cur == count-1)
		{
			int64_t cost = TC_Common::now2us() - start;
			cout << "callback_count count:" << count << ", " << cost << " us, avg:" << 1.*cost/count << "us" << endl;
		}
	}
	virtual void callback_get_exception(tars::Int32 ret)
	{
		cout << "callback_get_exception:" << ret << endl;
	}

	virtual void callback_set(tars::Int32 ret)
	{
		++callback;

		if(cur == count-1)
		{
			int64_t cost = TC_Common::now2us() - start;
			cout << "callback_count count:" << count << ", " << cost << " us, avg:" << 1.*cost/count << "us" << endl;
		}
	}

	virtual void callback_set_exception(tars::Int32 ret)
	{
		cout << "callback_set_exception:" << ret << endl;
	}

	virtual void callback_getBatch(tars::Int32 ret,  const vector<Base::StorageData>& rsp)
	{
		++callback;

		if(cur == count-1)
		{
			int64_t cost = TC_Common::now2us() - start;
			cout << "callback_count count:" << count << ", " << cost << " us, avg:" << 1.*cost/count << "us" << endl;
		}
	}

	virtual void callback_getBatch_exception(tars::Int32 ret)
	{
		cout << "callback_getBatch_exception:" << ret << endl;
	}

	virtual void callback_setBatch(tars::Int32 ret, const map<StorageKey, int> &rsp)
	{
		++callback;

		if(cur == count-1)
		{
			int64_t cost = TC_Common::now2us() - start;
			cout << "callback_count count:" << count << ", " << cost << " us, avg:" << 1.*cost/count << "us" << endl;
		}
	}

	virtual void callback_setBatch_exception(tars::Int32 ret)
	{
		cout << "callback_setBatch_exception:" << ret << endl;
	}

	virtual void callback_trans(tars::Int32 ret, const vector<Base::StorageData>& data)
	{
		++callback;

		if(cur == count-1)
		{
			int64_t cost = TC_Common::now2us() - start;
			cout << "callback_count count:" << count << ", " << cost << " us, avg:" << 1.*cost/count << "us" << endl;
		}
	}

	virtual void callback_trans_exception(tars::Int32 ret)
	{
		cout << "callback_trans_exception:" << ret << endl;
	}

	int64_t start;
	int     cur;
	int     count;
};

void syncGetCall(int c)
{
	int64_t t = TC_Common::now2us();

	Options options;
	options.leader = false;

	//发起远程调用
	for (int i = 0; i < c; ++i)
	{
		try
		{
			StorageKey req;
			req.table = "test";
			req.mkey = "mkey";
			req.ukey = "ukey-" + TC_Common::tostr(i);

			StorageValue rsp;
			param.pPrx->get(options, req, rsp);

//			TC_Common::sleep(1);
		}
		catch(exception& e)
		{
			cout << "exception:" << e.what() << endl;
		}
		++callback;
	}

	int64_t cost = TC_Common::now2us() - t;
	cout << "syncCall total:" << cost << "us, avg:" << 1.*cost/c << "us" << endl;
}

void asyncGetCall(int c)
{
	int64_t t = TC_Common::now2us();

	Options options;
	options.leader = false;

	//发起远程调用
	for (int i = 0; i < c; ++i)
	{
		StoragePrxCallbackPtr p = new StorageCallback(t, i, c);

		StorageKey req;
		req.table = "test";
		req.mkey = "mkey";
		req.ukey = "ukey";

		try
		{
			param.pPrx->async_get(p, options, req);
		}
		catch(exception& e)
		{
			cout << "exception:" << e.what() << endl;
		}

		if(i - callback > 1000)
		{
			TC_Common::msleep(50);
		}
	}

	int64_t cost = TC_Common::now2us() - t;
	cout << "asyncCall send:" << cost << "us, avg:" << 1.*cost/c << "us" << endl;
}

void create(int c)
{
	for(int i = 0; i < c; i++)
	{
		param.pPrx->createTable("test");

		++callback;
	}
}

void ping(int c)
{
	for(int i = 0; i < c; i++)
	{
		param.pPrx->tars_ping();

		++callback;
	}
}
void syncSetCall(int c)
{
	int64_t t = TC_Common::now2us();

	//发起远程调用
	for (int i = 0; i < c; ++i)
	{
		try
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey = "mkey";
			data.skey.ukey = "ukey-" + TC_Common::tostr(i);

			param.pPrx->set(data);
		}
		catch(exception& e)
		{
			cout << "exception:" << e.what() << endl;
		}
		++callback;
	}

	int64_t cost = TC_Common::now2us() - t;
	cout << "syncCall total:" << cost << "us, avg:" << 1.*cost/c << "us" << endl;
}

void asyncSetCall(int c)
{
	int64_t t = TC_Common::now2us();

	//发起远程调用
	for (int i = 0; i < c; ++i)
	{
		StoragePrxCallbackPtr p = new StorageCallback(t, i, c);

		StorageData data;
		data.skey.table = "test";
		data.skey.mkey = "mkey";
		data.skey.ukey = "ukey-" + TC_Common::tostr(i);

		try
		{
			param.pPrx->async_set(p, data);
		}
		catch(exception& e)
		{
			cout << "exception:" << e.what() << endl;
		}

		if(i - callback > 1000)
		{
			TC_Common::msleep(50);
		}
	}

	int64_t cost = TC_Common::now2us() - t;
	cout << "asyncCall send:" << cost << "us, avg:" << 1.*cost/c << "us" << endl;
}

void syncSetBatchCall(int c)
{
	int64_t t = TC_Common::now2us();

	vector<StorageData> value;
	for (int i = 0; i < 100; ++i)
	{
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey = "mkey";
		data.skey.ukey = "ukey-" + TC_Common::tostr(i);

		value.push_back(data);
	}

	//发起远程调用
	for (int i = 0; i < c; ++i)
	{
		try
		{

			map<StorageKey, int> rsp;
			param.pPrx->setBatch(value, rsp);
		}
		catch(exception& e)
		{
			cout << "exception:" << e.what() << endl;
		}
		++callback;
	}

	int64_t cost = TC_Common::now2us() - t;
	cout << "syncCall total:" << cost << "us, avg:" << 1.*cost/c << "us" << endl;
}

void asyncSetBatchCall(int c)
{
	int64_t t = TC_Common::now2us();

	vector<StorageData> value;
	for (int i = 0; i < 100; ++i)
	{
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey = "mkey";
		data.skey.ukey = "ukey-" + TC_Common::tostr(i);

		value.push_back(data);
	}

	//发起远程调用
	for (int i = 0; i < c; ++i)
	{
		StoragePrxCallbackPtr p = new StorageCallback(t, i, c);

		try
		{
			param.pPrx->async_setBatch(p, value);
		}
		catch(exception& e)
		{
			cout << "exception:" << e.what() << endl;
		}

		if(i - callback > 1000)
		{
			TC_Common::msleep(50);
		}
	}

	int64_t cost = TC_Common::now2us() - t;
	cout << "asyncCall send:" << cost << "us, avg:" << 1.*cost/c << "us" << endl;
}

void syncGetBatchCall(int c)
{
	int64_t t = TC_Common::now2us();

	vector<StorageKey> skey;
	vector<StorageData> value;

	for (int i = 0; i < 100; ++i)
	{
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey = "mkey";
		data.skey.ukey = "ukey-" + TC_Common::tostr(i);

		skey.push_back(data.skey);
	}

	Options options;
	options.leader = false;

	//发起远程调用
	for (int i = 0; i < c; ++i)
	{
		try
		{

			param.pPrx->getBatch(options, skey, value);
		}
		catch(exception& e)
		{
			cout << "exception:" << e.what() << endl;
		}
		++callback;
	}

	int64_t cost = TC_Common::now2us() - t;
	cout << "syncCall total:" << cost << "us, avg:" << 1.*cost/c << "us" << endl;
}

void asyncGetBatchCall(int c)
{
	int64_t t = TC_Common::now2us();

	vector<StorageData> value;
	for (int i = 0; i < 100; ++i)
	{
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey = "mkey";
		data.skey.ukey = "ukey-" + TC_Common::tostr(i);

		value.push_back(data);
	}

	//发起远程调用
	for (int i = 0; i < c; ++i)
	{
		StoragePrxCallbackPtr p = new StorageCallback(t, i, c);

		try
		{
			param.pPrx->async_setBatch(p, value);
		}
		catch(exception& e)
		{
			cout << "exception:" << e.what() << endl;
		}

		if(i - callback > 1000)
		{
			TC_Common::msleep(50);
		}
	}

	int64_t cost = TC_Common::now2us() - t;
	cout << "asyncCall send:" << cost << "us, avg:" << 1.*cost/c << "us" << endl;
}


void syncTransCall(int c)
{
	int64_t t = TC_Common::now2us();

	PageReq req;

	vector<StorageData> data;

	req.skey.table = "test";
	req.skey.mkey = "mkey";
	req.skey.ukey = "ukey-" + TC_Common::tostr(0);
	req.limit = 10;

	Options options;
	options.leader = false;

	//发起远程调用
	for (int i = 0; i < c; ++i)
	{
		try
		{

			param.pPrx->trans(options, req, data);
		}
		catch(exception& e)
		{
			cout << "exception:" << e.what() << endl;
		}
		++callback;
	}

	int64_t cost = TC_Common::now2us() - t;
	cout << "syncCall total:" << cost << "us, avg:" << 1.*cost/c << "us" << endl;
}

void asyncTransCall(int c)
{
	int64_t t = TC_Common::now2us();

	PageReq req;

	req.skey.table = "test";
	req.skey.mkey = "mkey";
	req.skey.ukey = "ukey-" + TC_Common::tostr(0);
	req.limit = 10;

	Options options;
	options.leader = false;

	//发起远程调用
	for (int i = 0; i < c; ++i)
	{
		StoragePrxCallbackPtr p = new StorageCallback(t, i, c);

		try
		{
			param.pPrx->async_trans( p, options, req);
		}
		catch(exception& e)
		{
			cout << "exception:" << e.what() << endl;
		}

		if(i - callback > 1000)
		{
			TC_Common::msleep(50);
		}
	}

	int64_t cost = TC_Common::now2us() - t;
	cout << "asyncCall send:" << cost << "us, avg:" << 1.*cost/c << "us" << endl;
}


int main(int argc, char *argv[])
{
	try
	{
		if (argc < 2)
		{
			cout << "Usage:" << argv[0] << " --count=1000 --call=[create|ping|set-sync|set-async|get-sync|get-async|getBatch-sync|setBatch-async|sync-trans|async-trans] --thread=1" << endl;

			return 0;
		}

		TC_Option option;
		option.decode(argc, argv);

		param.count = TC_Common::strto<int>(option.getValue("count"));
		if(param.count <= 0) param.count = 1000;
		param.call = option.getValue("call");
		if(param.call.empty()) param.call = "sync";
		param.thread = TC_Common::strto<int>(option.getValue("thread"));
		if(param.thread <= 0) param.thread = 1;

		_comm = new Communicator();

        LocalRollLogger::getInstance()->logger()->setLogLevel(6);

		_comm->setProperty("sendqueuelimit", "1000000");
		_comm->setProperty("asyncqueuecap", "1000000");

		param.pPrx = _comm->stringToProxy<StoragePrx>(matchObj);

		param.pPrx->tars_connect_timeout(50000);
		param.pPrx->tars_set_timeout(60 * 1000);
		param.pPrx->tars_async_timeout(60*1000);
//		param.pPrx->tars_ping();

		int64_t start = TC_Common::now2us();

		std::function<void(int)> func;

		if (param.call == "ping")
		{
			func = ping;
		}
		else if (param.call == "create")
		{
			func = create;
		}
		else if (param.call == "set-sync")
		{
			func = syncSetCall;
		}
		else if (param.call == "set-async")
		{
			func = asyncSetCall;
		}
		else if (param.call == "get-sync")
		{
			func = syncGetCall;
		}
		else if (param.call == "set-async")
		{
			func = asyncGetCall;
		}
		else if (param.call == "getBatch-sync")
		{
			func = syncGetBatchCall;
		}
		else if (param.call == "setBatch-async")
		{
			func = asyncSetBatchCall;
		}
		else if (param.call == "sync-trans")
		{
			func = syncTransCall;
		}
		else if (param.call == "async-trans")
		{
			func = asyncTransCall;
		}

		vector<std::thread*> vt;
		for(int i = 0 ; i< param.thread; i++)
		{
			vt.push_back(new std::thread(func, param.count));
		}

		std::thread print([&]{while(callback != param.count * param.thread) {
			cout << "Auth:" << param.call << " : ----------finish count:" << callback << endl;
			std::this_thread::sleep_for(std::chrono::seconds(1));
		};});

		for(size_t i = 0 ; i< vt.size(); i++)
		{
			vt[i]->join();
			delete vt[i];
		}

		cout << "(pid:" << std::this_thread::get_id() << ")"
		     << "(count:" << param.count << ")"
		     << "(use ms:" << (TC_Common::now2us() - start)/1000 << ")"
		     << endl;


		while(callback != param.count * param.thread) {
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}

		print.join();
		cout << "order:" << param.call << " ----------finish count:" << callback << endl;
	}
	catch(exception &ex)
	{
		cout << ex.what() << endl;
	}
	cout << "main return." << endl;

	return 0;
}
