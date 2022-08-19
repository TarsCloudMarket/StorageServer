#include <cassert>
#include <iostream>
#include <vector>
#include "gtest/gtest.h"
#include "rafttest/RaftTest.h"
#include "StorageServer.h"
#include "Storage.h"
#include "Json.h"

using namespace std;
using namespace Base;

class StorageUnitTest : public testing::Test
{

public:
	StorageUnitTest()
	{
	}

	~StorageUnitTest()
	{
	}

	static void SetUpTestCase()
	{
	}
	static void TearDownTestCase()
	{

	}

	virtual void SetUp()
	{

	}

	virtual void TearDown()
	{
	}

	void testCreateQueue(const shared_ptr<StorageServer> &server)
	{
		StoragePrx prx = server->node()->getBussLeaderPrx<StoragePrx>();
		int ret = prx->createQueue("test");
		ASSERT_TRUE(ret == 0);
	}

	void testCreateTable(const shared_ptr<StorageServer> &server)
	{
		StoragePrx prx = server->node()->getBussLeaderPrx<StoragePrx>();
		int ret = prx->createTable("test");
		ASSERT_TRUE(ret == 0);
	}

	inline void testSet(const shared_ptr<StorageServer> &server)
	{
		try
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey = "abc";
			data.skey.ukey = "1";
			data.svalue.data.resize(100);

			StoragePrx prx = server->node()->getBussLeaderPrx<StoragePrx>();

			int ret = prx->set(data);

			ASSERT_TRUE(ret == 0);

		}
		catch (exception &ex) {
			LOG_CONSOLE_DEBUG << ex.what() << endl;
		}
	}

};

TEST_F(StorageUnitTest, TestRaft_Set)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->setBussFunc(std::bind(&StorageUnitTest::testSet, this, std::placeholders::_1));
	raftTest->setInitFunc(std::bind(&StorageUnitTest::testCreateTable, this, std::placeholders::_1));

	raftTest->testAll();
}

TEST_F(StorageUnitTest, TestStorageCreateTable)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	ret = prx->createTable("test");
	ASSERT_TRUE(ret == S_OK);
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == S_TABLE_EXIST);

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestStorageSetNoTable)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	StorageData data;
	data.skey.table = "test";
	data.skey.mkey 	= "abc";
	data.skey.ukey 	= "1";

	ret = prx->set(data);
	ASSERT_TRUE(ret == S_TABLE_NOT_EXIST);

	//测试批量写
	vector<StorageData> vdata;
	for(size_t i = 0; i < 10; i++)
	{
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey 	= "abc";
		data.skey.ukey 	= TC_Common::tostr(i);
		vdata.push_back(data);
	}

	map<StorageKey, int> rsp;

	ret = prx->setBatch(vdata,  rsp);
	ASSERT_TRUE(ret == S_TABLE_NOT_EXIST);

	raftTest->stopAll();

}

TEST_F(StorageUnitTest, TestStorageSetGet)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');

	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	{
		//测试写
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey 	= "abc";
		data.skey.ukey 	= "1";
		data.svalue.data= v;

//		TC_Common::sleep(1000);
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		//测试读
		StorageKey skey;
		skey.table = "test";
		skey.mkey = "abc";
		skey.ukey = "1";

		Base::StorageValue value;

		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);
		ASSERT_TRUE(value.data == v);

		//测试del
		ret = prx->del(skey);
		ASSERT_TRUE(ret == 0);

		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == S_NO_DATA);

	}

	raftTest->stopAll();
}


TEST_F(StorageUnitTest, TestStorageSetGetBatch)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');

	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	{
		//测试批量写
		vector<StorageKey> skey;

		for(size_t i = 0; i < 10; i++)
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey 	= "abc";
			data.skey.ukey 	= TC_Common::tostr(i);
			data.svalue.data= v;

			ret = prx->set(data);
			ASSERT_TRUE(ret == 0);

			skey.push_back(data.skey);

			Base::StorageValue value;

			ret = prx->get(options, skey[i], value);
			ASSERT_TRUE(ret == 0);
			ASSERT_TRUE(value.data == v);
		}


		vector<StorageData> rdata;
		ret = prx->getBatch(options, skey, rdata);
		ASSERT_TRUE(ret == 0);

		ASSERT_TRUE(rdata.size() == skey.size());
		for(auto &data : rdata)
		{
			ASSERT_TRUE(data.ret == 0);
			ASSERT_TRUE(data.svalue.data == v);
		}
	}

	raftTest->stopAll();

}

TEST_F(StorageUnitTest, TestStorageSetBatchGetBatch)
{
	try
	{

		auto raftTest = std::make_shared<RaftTest<StorageServer>>();
		raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
		raftTest->createServers(3);

		raftTest->startAll();

		raftTest->waitCluster();

		vector<char> v;
		v.resize(100, 'a');

		Options options;
		options.leader = false;

		int ret;
		StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
		ret = prx->createTable("test");
		ASSERT_TRUE(ret == 0);

		{
			//测试批量写
			vector<StorageData> vdata;
			for(size_t i = 0; i < 2; i++)
			{
				StorageData data;
				data.skey.table = "test";
				data.skey.mkey 	= "abc";
				data.skey.ukey 	= TC_Common::tostr(i);
				data.svalue.data= v;

				vdata.push_back(data);
			}

			map<StorageKey, int> rsp;

			ret = prx->setBatch(vdata,  rsp);

			ASSERT_TRUE(ret == 0);
			ASSERT_TRUE(vdata.size() == rsp.size());
			for(auto k : rsp)
			{
				ASSERT_TRUE(k.second == S_OK);
			}

			vector<StorageKey> skey;
			for(auto &data : vdata)
			{
				skey.push_back(data.skey);
			}

			vector<StorageData> rdata;
			ret = prx->getBatch(options, skey, rdata);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(rdata.size() == skey.size());
			for(auto &data : rdata)
			{
				ASSERT_TRUE(data.ret == 0);
				ASSERT_TRUE(data.svalue.data == v);
			}

			ret = prx->delBatch(skey);
			ASSERT_TRUE(ret == 0);

			ret = prx->getBatch(options, skey, rdata);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(rdata.size() == skey.size());
			for(auto &data : rdata)
			{
				ASSERT_TRUE(data.ret == S_NO_DATA);
			}
		}

		raftTest->stopAll();
	}
	catch(exception &ex)
	{
		LOG_CONSOLE_DEBUG << ex.what() << endl;
	}
}

TEST_F(StorageUnitTest, TestStorageHas)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');

	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	{
		//测试读
		StorageKey skey;
		skey.table = "test";
		skey.mkey = "abc";
		skey.ukey = "1";

		ret = prx->has(options, skey);

		ASSERT_TRUE(ret == S_NO_DATA);

		//测试写
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey 	= "abc";
		data.skey.ukey 	= "1";
		data.svalue.data= v;

		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		ret = prx->has(options, skey);

		ASSERT_TRUE(ret == S_OK);

	}

	raftTest->stopAll();
}


TEST_F(StorageUnitTest, TestStorageHasBatch)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');

	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);
	{
		vector<StorageKey> skeys;
		for(int i = 0; i < 100; i++)
		{
			//测试读
			StorageKey skey;
			skey.table = "test";
			skey.mkey = "abc";
			skey.ukey = TC_Common::tostr(i);

			skeys.push_back(skey);
		}

		map<StorageKey, int> rsp;
		ret = prx->hasBatch(options, skeys, rsp);

		ASSERT_TRUE(ret == S_OK);
		for(auto e : rsp)
		{
			ASSERT_TRUE(e.second == S_NO_DATA);
		}

		vector<StorageData> sdata;
		for(int i = 0; i < 100; i++)
		{
			//测试读
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey 	= "abc";
			data.skey.ukey 	= TC_Common::tostr(i);

			sdata.push_back(data);
		}

		rsp.clear();
		ret = prx->setBatch(sdata, rsp);
		ASSERT_TRUE(ret == 0);

		ret = prx->hasBatch(options, skeys, rsp);
		ASSERT_TRUE(ret == 0);
		for(auto e : rsp)
		{
			ASSERT_TRUE(e.second == S_OK);
		}
	}

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestStorageSetGetVersion)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');
	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	{
		//测试写
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey 	= "abc";
		data.skey.ukey 	= "1";
		data.svalue.data= v;

		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		//测试读
		StorageKey skey;
		skey.table = "test";
		skey.mkey = "abc";
		skey.ukey = "1";

		Base::StorageValue value;

		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);
		ASSERT_TRUE(value.data == v);

		data.svalue.version = rand();

		ret = prx->set(data);
		ASSERT_TRUE(ret == S_VERSION);

		data.svalue.version = value.version;
		ret = prx->set(data);
		ASSERT_TRUE(ret == S_OK);

	}

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestStorageSetGetTimestamp)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');
	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	{
		//测试写
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey 	= "abc";
		data.skey.ukey 	= "1";
		data.svalue.timestamp = TC_Common::now2us();
		data.svalue.data= v;

		ret = prx->set(data);
		LOG_CONSOLE_DEBUG << ret << endl;
		ASSERT_TRUE(ret == 0);

		//测试读
		StorageKey skey;
		skey.table = "test";
		skey.mkey = "abc";
		skey.ukey = "1";

		Base::StorageValue value;

		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);
		ASSERT_TRUE(value.data == v);

		--data.svalue.timestamp;
		ret = prx->set(data);
		ASSERT_TRUE(ret == S_TIMESTAMP);

		data.svalue.timestamp = TC_Common::now2us();
		ret = prx->set(data);
		ASSERT_TRUE(ret == S_OK);
	}

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestStorageSetGetTimestamp1)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');
	Options options;
	options.leader = true;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	{
		//测试写
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey 	= "abc";
		data.skey.ukey 	= "1";
		data.svalue.version = 1;
		data.svalue.expireTime = 0;
		data.svalue.timestamp = 232323;
		data.svalue.data= v;

		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		//测试读
		StorageKey skey;
		skey.table = "test";
		skey.mkey = "abc";
		skey.ukey = "1";

		Base::StorageValue value;

		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);
		ASSERT_TRUE(value.data == v);

		auto now = TNOW;
		data.svalue.expireTime = 1000;
		++data.svalue.timestamp;
		ret = prx->set(data);

		ASSERT_TRUE(ret == S_OK);

		Base::StorageValue value1;
		prx->get(options, skey, value1);
		ASSERT_TRUE(ret == 0);

		ASSERT_TRUE(value1.expireTime >= now);

	}

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestStorageSetGetBatchVersion)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');
	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	{
		//测试批量写
		vector<StorageData> vdata;
		for(size_t i = 0; i < 100; i++)
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey 	= "abc";
			data.skey.ukey 	= TC_Common::tostr(i);
			data.svalue.data= v;

			vdata.push_back(data);
		}

		map<StorageKey, int> rsp;

		ret = prx->setBatch(vdata,  rsp);

		ASSERT_TRUE(ret == 0);
		ASSERT_TRUE(vdata.size() == rsp.size());
		for(auto k : rsp)
		{
			ASSERT_TRUE(k.second == S_OK);
		}

		vector<StorageKey> skey;
		for(auto &data : vdata)
		{
			skey.push_back(data.skey);
		}

		vector<StorageData> rdata;
		ret = prx->getBatch(options, skey, rdata);
		ASSERT_TRUE(ret == 0);

		ASSERT_TRUE(rdata.size() == skey.size());
		for(auto &data : rdata)
		{
			ASSERT_TRUE(data.ret == 0);
			ASSERT_TRUE(data.svalue.data == v);
		}

		for(auto &data : vdata)
		{
			data.svalue.version = rand();
		}

		rsp.clear();

		ret = prx->setBatch(vdata,  rsp);
		ASSERT_TRUE(ret == S_OK);
		ASSERT_TRUE(vdata.size() == rsp.size());
		for(auto k : rsp)
		{
			ASSERT_TRUE(k.second == S_VERSION);
		}

	}

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestStorageSetGetBatchTimestamp)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');
	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);
	{
		//测试批量写
		vector<StorageData> vdata;
		for(size_t i = 0; i < 100; i++)
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey 	= "abc";
			data.skey.ukey 	= TC_Common::tostr(i);
			data.svalue.data= v;

			vdata.push_back(data);
		}

		map<StorageKey, int> rsp;

		ret = prx->setBatch(vdata,  rsp);

		ASSERT_TRUE(ret == 0);
		ASSERT_TRUE(vdata.size() == rsp.size());
		for(auto k : rsp)
		{
			ASSERT_TRUE(k.second == S_OK);
		}

		vector<StorageKey> skey;
		for(auto &data : vdata)
		{
			skey.push_back(data.skey);
		}

		vector<StorageData> rdata;
		ret = prx->getBatch(options, skey, rdata);
		ASSERT_TRUE(ret == 0);

		ASSERT_TRUE(rdata.size() == skey.size());
		for(auto &data : rdata)
		{
			ASSERT_TRUE(data.ret == 0);
			ASSERT_TRUE(data.svalue.data == v);
		}

		for(auto &data : vdata)
		{
			--data.svalue.timestamp;
		}

		rsp.clear();

		ret = prx->setBatch(vdata,  rsp);
		ASSERT_TRUE(ret == S_OK);
		ASSERT_TRUE(vdata.size() == rsp.size());
		for(auto k : rsp)
		{
			ASSERT_TRUE(k.second == S_TIMESTAMP);
		}

		for(auto &data : vdata)
		{
			data.svalue.timestamp += 2;
		}

		rsp.clear();

		ret = prx->setBatch(vdata,  rsp);
		ASSERT_TRUE(ret == 0);
		ASSERT_TRUE(vdata.size() == rsp.size());
		for(auto k : rsp)
		{
			ASSERT_TRUE(k.second == S_OK);
		}
	}

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestStorageTransForward)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');
	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	{
		//测试批量写
		vector<StorageData> vdata;
		for(size_t i = 0; i < 100; i++)
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey 	= "aaa";
			data.skey.ukey 	= TC_Common::outfill(TC_Common::tostr(i), '0', 5, false);
			data.svalue.data= v;

			vdata.push_back(data);
		}
		for(size_t i = 0; i < 100; i++)
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey 	= "abc";
			data.skey.ukey 	= TC_Common::outfill(TC_Common::tostr(i), '0', 5, false);
			data.svalue.data= v;

			vdata.push_back(data);
		}
		for(size_t i = 0; i < 100; i++)
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey 	= "def";
			data.skey.ukey 	= TC_Common::outfill(TC_Common::tostr(i), '0', 5, false);
			data.svalue.data= v;

			vdata.push_back(data);
		}
		map<StorageKey, int> rsp;

		ret = prx->setBatch(vdata,  rsp);
		ASSERT_TRUE(ret == 0);

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "abc";
			req.skey.ukey = TC_Common::outfill("1", '0', 5, false);

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(data.size() == 10);
			ASSERT_TRUE(data[0].skey.mkey == "abc");
			ASSERT_TRUE(data[0].skey.ukey == "00001");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "abc");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00010");
		}

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "abc";
			req.skey.ukey = "";

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(data.size() == 10);
			ASSERT_TRUE(data[0].skey.mkey == "abc");
			ASSERT_TRUE(data[0].skey.ukey == "00000");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "abc");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00009");
		}

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "abc";
			req.skey.ukey = "00095";
			req.limit = 12;

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(data.size() == 5);
			ASSERT_TRUE(data[0].skey.mkey == "abc");
			ASSERT_TRUE(data[0].skey.ukey == "00095");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "abc");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00099");
		}

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "";
			req.skey.ukey = "";
			req.limit = -1;
			req.over = true;

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			LOG_CONSOLE_DEBUG << data.size() << endl;
			ASSERT_TRUE(data.size() == 300);
			ASSERT_TRUE(data[0].skey.mkey == "aaa");
			ASSERT_TRUE(data[0].skey.ukey == "00000");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "def");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00099");
		}

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "";
			req.skey.ukey = "";
			req.limit = 102;
			req.over = true;

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(data.size() == req.limit);
			ASSERT_TRUE(data[0].skey.mkey == "aaa");
			ASSERT_TRUE(data[0].skey.ukey == "00000");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "abc");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00001");

			{
				PageReq req;
				req.skey.table = "test";
				req.skey.mkey = data[data.size()-1].skey.mkey;
				req.skey.ukey = data[data.size()-1].skey.ukey;
				req.include = false;
				req.limit = 102;
				req.over = true;

				vector<StorageData> data;

				ret = prx->trans(options, req, data);
				ASSERT_TRUE(ret == 0);

				ASSERT_TRUE(data.size() == req.limit);
				ASSERT_TRUE(data[0].skey.mkey == "abc");
				ASSERT_TRUE(data[0].skey.ukey == "00002");
				ASSERT_TRUE(data[data.size()-1].skey.mkey == "def");
				ASSERT_TRUE(data[data.size()-1].skey.ukey == "00003");
			}

			{
				PageReq req;
				req.skey.table = "test";
				req.skey.mkey = data[data.size()-1].skey.mkey;
				req.skey.ukey = data[data.size()-1].skey.ukey;
				req.include = true;
				req.limit = 102;
				req.over = true;

				vector<StorageData> data;

				ret = prx->trans(options, req, data);
				ASSERT_TRUE(ret == 0);

				ASSERT_TRUE(data.size() == req.limit);
				ASSERT_TRUE(data[0].skey.mkey == "abc");
				ASSERT_TRUE(data[0].skey.ukey == "00001");
				ASSERT_TRUE(data[data.size()-1].skey.mkey == "def");
				ASSERT_TRUE(data[data.size()-1].skey.ukey == "00002");
			}
		}
	}

	raftTest->stopAll();
}


TEST_F(StorageUnitTest, TestStorageTransBackward)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');
	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	{
		//测试批量写
		vector<StorageData> vdata;
		for(size_t i = 0; i < 100; i++)
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey 	= "aaa";
			data.skey.ukey 	= TC_Common::outfill(TC_Common::tostr(i), '0', 5, false);
			data.svalue.data= v;

			vdata.push_back(data);
		}

		for(size_t i = 0; i < 100; i++)
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey 	= "abc";
			data.skey.ukey 	= TC_Common::outfill(TC_Common::tostr(i), '0', 5, false);
			data.svalue.data= v;

			vdata.push_back(data);
		}

		for(size_t i = 0; i < 100; i++)
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey 	= "def";
			data.skey.ukey 	= TC_Common::outfill(TC_Common::tostr(i), '0', 5, false);
			data.svalue.data= v;

			vdata.push_back(data);
		}

		map<StorageKey, int> rsp;

		ret = prx->setBatch(vdata,  rsp);
		ASSERT_TRUE(ret == 0);

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "abc";
			req.skey.ukey = TC_Common::outfill("99", '0', 5, false);
			req.forward = false;

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(data.size() == 10);
			ASSERT_TRUE(data[0].skey.mkey == "abc");
			ASSERT_TRUE(data[0].skey.ukey == "00099");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "abc");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00090");
		}

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "abc";
			req.skey.ukey = "";
			req.limit = 10;
			req.forward = false;

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(data.size() == 10);
			ASSERT_TRUE(data[0].skey.mkey == "abc");
			ASSERT_TRUE(data[0].skey.ukey == "00099");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "abc");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00090");
		}

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "abc";
			req.skey.ukey = "00024";
			req.limit = 12;
			req.forward = false;

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(data.size() == 12);
			ASSERT_TRUE(data[0].skey.mkey == "abc");
			ASSERT_TRUE(data[0].skey.ukey == "00024");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "abc");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00013");
		}

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "abc";
			req.skey.ukey = "00005";
			req.limit = 12;
			req.forward = false;

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(data.size() == 6);
			ASSERT_TRUE(data[0].skey.mkey == "abc");
			ASSERT_TRUE(data[0].skey.ukey == "00005");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "abc");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00000");
		}

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "";
			req.skey.ukey = "";
			req.limit = -1;
			req.forward = false;
			req.over = true;

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			LOG_CONSOLE_DEBUG << data.size() << endl;
			ASSERT_TRUE(data.size() == 300);
			ASSERT_TRUE(data[0].skey.mkey == "def");
			ASSERT_TRUE(data[0].skey.ukey == "00099");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "aaa");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00000");

		}

		{
			PageReq req;
			req.skey.table = "test";
			req.skey.mkey = "";
			req.skey.ukey = "";
			req.limit = 102;
			req.forward = false;
			req.over = true;

			vector<StorageData> data;

			ret = prx->trans(options, req, data);
			ASSERT_TRUE(ret == 0);

			ASSERT_TRUE(data.size() == req.limit);
			ASSERT_TRUE(data[0].skey.mkey == "def");
			ASSERT_TRUE(data[0].skey.ukey == "00099");
			ASSERT_TRUE(data[data.size()-1].skey.mkey == "abc");
			ASSERT_TRUE(data[data.size()-1].skey.ukey == "00098");

			{
				PageReq req;
				req.skey.table = "test";
				req.skey.mkey = data[data.size()-1].skey.mkey;
				req.skey.ukey = data[data.size()-1].skey.ukey;
				req.limit = 102;
				req.include = false;
				req.forward = false;
				req.over = true;

				vector<StorageData> data;

				ret = prx->trans(options, req, data);
				ASSERT_TRUE(ret == 0);

				ASSERT_TRUE(data.size() == req.limit);
				ASSERT_TRUE(data[0].skey.mkey == "abc");
				ASSERT_TRUE(data[0].skey.ukey == "00097");
				ASSERT_TRUE(data[data.size()-1].skey.mkey == "aaa");
				ASSERT_TRUE(data[data.size()-1].skey.ukey == "00096");
			}

			{
				PageReq req;
				req.skey.table = "test";
				req.skey.mkey = data[data.size()-1].skey.mkey;
				req.skey.ukey = data[data.size()-1].skey.ukey;
				req.limit = 102;
				req.include = true;
				req.forward = false;
				req.over = true;

				vector<StorageData> data;

				ret = prx->trans(options, req, data);
				ASSERT_TRUE(ret == 0);

				ASSERT_TRUE(data.size() == req.limit);
				ASSERT_TRUE(data[0].skey.mkey == "abc");
				ASSERT_TRUE(data[0].skey.ukey == "00098");
				ASSERT_TRUE(data[data.size()-1].skey.mkey == "aaa");
				ASSERT_TRUE(data[data.size()-1].skey.ukey == "00097");
			}
		}
	}

	raftTest->stopAll();
}


TEST_F(StorageUnitTest, TestStorageExpireTime)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');
	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	StorageData data;
	data.skey.table = "test";
	data.skey.mkey 	= "abc";
	data.skey.ukey 	= "0";
	data.svalue.data= v;
	data.svalue.expireTime = 2;

	ret = prx->set(data);
	ASSERT_TRUE(ret == 0);

	Base::StorageValue value;

	ret = prx->get(options, data.skey, value);
	ASSERT_TRUE(ret == 0);
	ASSERT_TRUE(value.data == v);

	TC_Common::sleep(4);

	ret = prx->get(options, data.skey, value);
	ASSERT_TRUE(ret == S_NO_DATA);

	ret = prx->has(options, data.skey);
	ASSERT_TRUE(ret == S_NO_DATA);

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestStorageExpireTimeBatch)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	vector<char> v;
	v.resize(100, 'a');
	Options options;
	options.leader = false;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();
	ret = prx->createTable("test");
	ASSERT_TRUE(ret == 0);

	vector<StorageData> vdata;
	for(size_t i = 0; i < 10; i++)
	{
		StorageData data;
		data.skey.table = "test";
		data.skey.mkey 	= "abc";
		data.skey.ukey 	= TC_Common::tostr(i);
		data.svalue.data= v;
		data.svalue.expireTime = 2;

		vdata.push_back(data);
	}

	map<Base::StorageKey, tars::Int32> rsp;
	ret = prx->setBatch(vdata, rsp);

	ASSERT_TRUE(ret == 0);

	TC_Common::sleep(4);

	vector<StorageKey> vkey;

	for(size_t i = 0; i < 10; i++)
	{
		StorageKey key;
		key.table 	= "test";
		key.mkey 	= "abc";
		key.ukey 	= TC_Common::tostr(i);

		vkey.push_back(key);
	}

	vector<Base::StorageData> value;
	ret = prx->getBatch(options, vkey, value);
	ASSERT_TRUE(ret == S_OK);

	for(auto &v : value)
	{
		ASSERT_TRUE(v.ret == S_NO_DATA);
	}

	raftTest->stopAll();
}


TEST_F(StorageUnitTest, TestStorageJson)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	LOG_CONSOLE_DEBUG << "wait ok" << endl;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	prx->createTable("test_json");
	LOG_CONSOLE_DEBUG << "createTable ok" << endl;

	StorageKey skey;
	skey.table = "test_json";
	skey.mkey = "test";

	StorageData data;
	data.skey = skey;
//	data.svalue.expireTime = 10000;

	Options options;
	options.leader = true;

	int ret;

	TestJson  tj;
	tj.str = "abc";
	tj.boon = true;
	tj.inte = 10;
	tj.doub = 11.f;
	tj.strs.push_back("def");
	tj.intes.push_back(10);
	tj.doubs.push_back(11.f);

	string buff = tj.writeToJsonString();
	data.svalue.data.insert(data.svalue.data.end(), buff.begin(), buff.end());
	LOG_CONSOLE_DEBUG << "begin set" << endl;

	ret = prx->set(data);
	ASSERT_TRUE(ret == 0);
	LOG_CONSOLE_DEBUG << "set ok" << endl;

	{
		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		ASSERT_TRUE(rtj.str == tj.str);
	}

	if(0)
	{

		StorageJson json;
		json.skey = skey;

		StorageUpdate update;
		update.type = Base::FT_STRING;
		update.value = "def";
		update.field = "str";
		update.op = SO_REPLACE;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		ASSERT_TRUE(rtj.str == "def");
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_STRING;
		update.value = "def";
		update.field = "str";
		update.op = Base::SO_ADD;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		LOG_CONSOLE_DEBUG << "begin update" << endl;

		ret = prx->update(json);
		LOG_CONSOLE_DEBUG << "update ok" << endl;

		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << rtj.str << endl;

		ASSERT_TRUE(rtj.str == "abcdef");
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_STRING;
		update.value = "def";
		update.field = "str";
		update.op = Base::SO_ADD;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << rtj.str << endl;

		ASSERT_TRUE(rtj.str == "abcdef");
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_INTEGER;
		update.value = "8";
		update.field = "inte";
		update.op = Base::SO_REPLACE;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << rtj.inte << endl;

		ASSERT_TRUE(rtj.inte == 8);
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_INTEGER;
		update.value = "5";
		update.field = "inte";
		update.op = Base::SO_ADD;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << rtj.inte << endl;

		ASSERT_TRUE(rtj.inte == 15);
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_INTEGER;
		update.value = "5";
		update.field = "inte";
		update.op = Base::SO_SUB;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << rtj.inte << endl;

		ASSERT_TRUE(rtj.inte == 5);
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_BOOLEAN;
		update.value = "true";
		update.field = "boon";
		update.op = Base::SO_REPLACE;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << rtj.inte << endl;

		ASSERT_TRUE(rtj.boon);
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_BOOLEAN;
		update.value = "true";
		update.field = "boon";
		update.op = Base::SO_REVERSE;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << rtj.inte << endl;

		ASSERT_FALSE(rtj.boon);
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_INTEGER;
		update.value = "8";
		update.field = "doub";
		update.op = Base::SO_REPLACE;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << rtj.doub << endl;

		ASSERT_TRUE(TC_Common::equal(rtj.doub, 8));
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_INTEGER;
		update.value = "5";
		update.field = "doub";
		update.op = Base::SO_ADD;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << rtj.doub << endl;

		ASSERT_TRUE(TC_Common::equal(rtj.doub, 16));
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_DOUBLE;
		update.value = "5.5";
		update.field = "doub";
		update.op = Base::SO_SUB;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << rtj.doub << endl;

		ASSERT_TRUE(TC_Common::equal(rtj.doub, 5.5));
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_STRING;
		update.value = "55";
		update.field = "strs";
		update.op = Base::SO_ADD;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << TC_Common::tostr(rtj.strs.begin(), rtj.strs.end(), ", ") << endl;

		ASSERT_TRUE(rtj.strs.size() == 2);
		ASSERT_TRUE(rtj.strs[1] == "55");
	}

	{
//		ret = prx->set(data);
//		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_STRING;
		update.value = "55";
		update.field = "strs";
		update.op = Base::SO_SUB;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << TC_Common::tostr(rtj.strs.begin(), rtj.strs.end(), ", ") << endl;

		ASSERT_TRUE(rtj.strs.size() == 1);
		ASSERT_TRUE(rtj.strs[0] == "def");
	}

	{
//		ret = prx->set(data);
//		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_STRING;
		update.value = "def";
		update.field = "strs";
		update.op = Base::SO_ADD_NO_REPEAT;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << TC_Common::tostr(rtj.strs.begin(), rtj.strs.end(), ", ") << endl;

		ASSERT_TRUE(rtj.strs.size() == 1);
		ASSERT_TRUE(rtj.strs[0] == "def");
	}

	{
//		ret = prx->set(data);
//		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_ARRAY;
		update.value = "[\"3333\", \"111\"]";
		update.field = "strs";
		update.op = Base::SO_REPLACE;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << TC_Common::tostr(rtj.strs.begin(), rtj.strs.end(), ", ") << endl;

		ASSERT_TRUE(rtj.strs.size() == 2);
		ASSERT_TRUE(rtj.strs[1] == "111");
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_ARRAY;
		update.value = "[\"3333\", \"111\"]";
		update.field = "strs";
		update.op = Base::SO_ADD;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << TC_Common::tostr(rtj.strs.begin(), rtj.strs.end(), ", ") << endl;

		ASSERT_TRUE(rtj.strs.size() == 3);
		ASSERT_TRUE(rtj.strs[2] == "111");
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_ARRAY;
		update.value = "[\"def\", \"111\"]";
		update.field = "strs";
		update.op = Base::SO_ADD_NO_REPEAT;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		TestJson rtj;
		rtj.readFromJsonString(string(value.data.data(), value.data.size()));

		LOG_CONSOLE_DEBUG << TC_Common::tostr(rtj.strs.begin(), rtj.strs.end(), ", ") << endl;

		ASSERT_TRUE(rtj.strs.size() == 2);
		ASSERT_TRUE(rtj.strs[1] == "111");
	}

	{
		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		StorageUpdate update;
		update.type = Base::FT_ARRAY;
		update.value = "[\"def\", \"111\"]";
		update.field = "strs_new";
		update.op = Base::SO_ADD_NO_REPEAT;
		update.def = update.value;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);
		ASSERT_TRUE(ret == 0);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		JsonValuePtr pPtr = TC_Json::getValue(string(value.data.data(), value.data.size()));

		JsonValueObjPtr oPtr = JsonValueObjPtr::dynamicCast(pPtr);

		ASSERT_TRUE(oPtr->value.find("strs_new") != oPtr->value.end());

		JsonValueArrayPtr aPtr = JsonValueArrayPtr::dynamicCast(oPtr->value["strs_new"]);

		ASSERT_TRUE(aPtr->value.size() == 2);
		ASSERT_TRUE(JsonValueStringPtr::dynamicCast(aPtr->value[0])->value == "def");

	}

	{
		StorageUpdate update;
		update.type = Base::FT_STRING;
		update.value = "55";
		update.field = "strs_sub";
		update.op = Base::SO_SUB;

		StorageJson json;
		json.skey = skey;

		json.supdate.push_back(update);
		ret = prx->update(json);

		ASSERT_TRUE(ret == S_JSON_FIELD_NOT_EXITS);
	}

	{
		skey.mkey = "test_add";
		prx->del(skey);

		StorageUpdate update;
		update.type = Base::FT_ARRAY;
		update.value = "[\"55\"]";
		update.field = "strs_array_add";
		update.op = Base::SO_ADD_NO_REPEAT;
		update.def = update.value;

		StorageJson json;
		json.skey = skey;
		json.skey.mkey = "test_add";

		json.supdate.push_back(update);
		ret = prx->update(json);

		LOG_CONSOLE_DEBUG << ret << endl;

		ASSERT_TRUE(ret == S_OK);

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		JsonValuePtr pPtr = TC_Json::getValue(string(value.data.data(), value.data.size()));

		JsonValueObjPtr oPtr = JsonValueObjPtr::dynamicCast(pPtr);

		ASSERT_TRUE(oPtr->value.find("strs_array_add") != oPtr->value.end());
		JsonValueArrayPtr aPtr = JsonValueArrayPtr::dynamicCast(oPtr->value["strs_array_add"]);

		ASSERT_TRUE(aPtr->value.size() == 1);
		ASSERT_TRUE(JsonValueStringPtr::dynamicCast(aPtr->value[0])->value == "55");
	}

	raftTest->stopAll();
}


TEST_F(StorageUnitTest, TestQueueCreateQueue)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	ret = prx->createQueue("test");
	ASSERT_TRUE(ret == S_OK);
	ret = prx->createQueue("test");
	ASSERT_TRUE(ret == S_QUEUE_EXIST);

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestQueuePushBack)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	prx->createQueue("test");

	vector<Base::QueuePushReq> pushReq;
	vector<Base::QueueRsp> rsp;

	QueuePushReq req;
	req.queue = "test";
	req.back = true;

	req.data.assign(10, 'a');
	pushReq.push_back(req);

	req.data.assign(10, 'b');
	pushReq.push_back(req);

	req.data.assign(10, 'c');
	pushReq.push_back(req);

	ret = prx->push_queue(pushReq);

	ASSERT_TRUE(ret == S_OK);

	Base::Options options;
	options.leader = true;

	Base::QueuePopReq popReq;
	popReq.back = true;
	popReq.queue = "test";
	popReq.count = 2;

	ret = prx->get_queue(options, popReq, rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == popReq.count);
	ASSERT_TRUE(rsp[0].data == pushReq[pushReq.size()-1].data);
	ASSERT_TRUE(rsp[1].data == pushReq[pushReq.size()-2].data);

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestQueuePopBack)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	prx->createQueue("test");

	vector<Base::QueuePushReq> pushReq;
	vector<Base::QueueRsp> rsp;

	QueuePushReq req;
	req.queue = "test";
	req.back = true;

	req.data.assign(10, 'a');
	pushReq.push_back(req);

	req.data.assign(10, 'b');
	pushReq.push_back(req);

	req.data.assign(10, 'c');
	pushReq.push_back(req);

	LOG_CONSOLE_DEBUG << endl;
	ret = prx->push_queue(pushReq);

	ASSERT_TRUE(ret == S_OK);

	Base::Options options;
	options.leader = true;

	Base::QueuePopReq popReq;
	popReq.back = true;
	popReq.queue = "test";
	popReq.count = 2;

	rsp.clear();
	ret = prx->pop_queue(popReq, rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == popReq.count);
	ASSERT_TRUE(rsp[0].data == pushReq[pushReq.size()-1].data);
	ASSERT_TRUE(rsp[1].data == pushReq[pushReq.size()-2].data);

	rsp.clear();
	ret = prx->pop_queue(popReq, rsp);
	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == pushReq.size() - popReq.count);
	ASSERT_TRUE(rsp[0].data == pushReq[0].data);

	rsp.clear();
	ret = prx->pop_queue(popReq, rsp);
	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == 0);

	raftTest->stopAll();
}


TEST_F(StorageUnitTest, TestQueuePopFront)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	prx->createQueue("test");

	vector<Base::QueuePushReq> pushReq;
	vector<Base::QueueRsp> rsp;

	QueuePushReq req;
	req.queue = "test";
	req.back = false;

	req.data.assign(10, 'a');
	pushReq.push_back(req);

	req.data.assign(10, 'b');
	pushReq.push_back(req);

	req.data.assign(10, 'c');
	pushReq.push_back(req);

	ret = prx->push_queue(pushReq);

	ASSERT_TRUE(ret == S_OK);

	Base::Options options;
	options.leader = true;

	Base::QueuePopReq popReq;
	popReq.back = false;
	popReq.queue = "test";
	popReq.count = 2;

	rsp.clear();
	ret = prx->pop_queue(popReq, rsp);

	LOG_CONSOLE_DEBUG << rsp.size() << endl;

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == popReq.count);
	ASSERT_TRUE(rsp[0].data == pushReq[pushReq.size()-1].data);
	ASSERT_TRUE(rsp[1].data == pushReq[pushReq.size()-2].data);

	rsp.clear();
	ret = prx->pop_queue(popReq, rsp);
	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == pushReq.size() - popReq.count);
	ASSERT_TRUE(rsp[0].data == pushReq[0].data);

	rsp.clear();
	ret = prx->pop_queue(popReq, rsp);
	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == 0);

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestQueuePushFront)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	prx->createQueue("test");

	vector<Base::QueuePushReq> pushReq;
	vector<Base::QueueRsp> rsp;

	QueuePushReq req;
	req.queue = "test";
	req.back = false;

	req.data.assign(10, 'a');
	pushReq.push_back(req);

	req.data.assign(10, 'b');
	pushReq.push_back(req);

	req.data.assign(10, 'c');
	pushReq.push_back(req);

	ret = prx->push_queue(pushReq);

	ASSERT_TRUE(ret == S_OK);

	Base::Options options;
	options.leader = true;

	Base::QueuePopReq popReq;
	popReq.queue = "test";
	popReq.back = false;
	popReq.count = 2;

	rsp.clear();
	ret = prx->get_queue(options, popReq, rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == popReq.count);
	ASSERT_TRUE(rsp[0].data == pushReq[pushReq.size()-1].data);
	ASSERT_TRUE(rsp[1].data == pushReq[pushReq.size()-2].data);

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestQueuePushFrontExpireTime)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	prx->createQueue("test");

	vector<Base::QueuePushReq> pushReq;
	vector<Base::QueueRsp> rsp;

	QueuePushReq req;
	req.queue = "test";
	req.back = false;
	req.expireTime = 2;

	req.data.assign(10, 'a');
	pushReq.push_back(req);

	req.data.assign(10, 'b');
	pushReq.push_back(req);

	req.data.assign(10, 'c');
	pushReq.push_back(req);

	ret = prx->push_queue(pushReq);

	ASSERT_TRUE(ret == S_OK);

	TC_Common::sleep(4);

	Base::Options options;
	options.leader = true;

	Base::QueuePopReq popReq;
	popReq.queue = "test";
	popReq.back = false;
	popReq.count = 2;

	rsp.clear();
	ret = prx->get_queue(options, popReq, rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == 0);

	TC_Common::sleep(1);

	ret = prx->get_queue(options, popReq, rsp);
	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == 0);

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestQueueDeleteData)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	prx->createQueue("test");

	vector<Base::QueuePushReq> pushReq;
	vector<Base::QueueRsp> rsp;

	QueuePushReq req;
	req.queue = "test";
	req.back = false;
	req.data.assign(10, 'a');

	pushReq.push_back(req);
	ret = prx->push_queue(pushReq);

	ASSERT_TRUE(ret == S_OK);
	Base::Options options;
	options.leader = true;

	Base::QueuePopReq popReq;
	popReq.queue = "test";
	popReq.back = false;

	ret = prx->get_queue(options, popReq, rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp[0].data == req.data);

	vector<QueueIndex> indexReq;
	QueueIndex qi;
	qi.queue = "test";
	qi.index = rsp[0].index;

	indexReq.push_back(qi);

	rsp.clear();
	ret = prx->getQueueData(options, indexReq, rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp[0].data == req.data);

	ret = prx->deleteQueueData(indexReq);
	ASSERT_TRUE(ret == S_OK);
	rsp.clear();
	ret = prx->get_queue(options, popReq, rsp);

	LOG_CONSOLE_DEBUG << "size:" << rsp.size() << endl;

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == 0);

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestQueueDoBatch)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	Options options;
	options.leader = true;

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	prx->createTable("test");

	StorageKey skey;
	skey.table = "test";
	skey.mkey = "test";

	StorageData data;
	data.skey = skey;

	TestJson  tj;
	tj.str = "abc";
	tj.boon = true;
	tj.inte = 10;
	tj.doub = 11.f;
	tj.strs.push_back("def");
	tj.intes.push_back(10);
	tj.doubs.push_back(11.f);

	string buff = tj.writeToJsonString();
	data.svalue.data.insert(data.svalue.data.end(), buff.begin(), buff.end());

	ret = prx->set(data);
	ASSERT_TRUE(ret == 0);

	ret = prx->createQueue("test");
	ASSERT_TRUE(ret == 0);

	BatchDataReq batchReq;
	{

		vector<char> v;
		v.resize(100, 'a');

		vector<StorageData> vdata;
		for (size_t i = 0; i < 10; i++)
		{
			StorageData data;
			data.skey.table = "test";
			data.skey.mkey = "abc";
			data.skey.ukey = TC_Common::tostr(i);
			data.svalue.data = v;

			vdata.push_back(data);
		}

		batchReq.sData = vdata;
	}

	{
		vector<Base::QueuePushReq> pushReq;
		vector<Base::QueueRsp> rsp;

		QueuePushReq req;
		req.queue = "test";
		req.back = false;

		req.data.assign(10, 'a');
		pushReq.push_back(req);

		req.data.assign(10, 'b');
		pushReq.push_back(req);

		req.data.assign(10, 'c');
		pushReq.push_back(req);

		batchReq.qData = pushReq;
	}

	{
		{
			StorageUpdate update;
			update.type = Base::FT_STRING;
			update.value = "xxxx";
			update.field = "str";
			update.op = Base::SO_ADD;

			StorageJson json;
			json.skey = skey;

			json.supdate.push_back(update);

			batchReq.uData.push_back(json);
		}

		{
			StorageUpdate update;
			update.type = Base::FT_INTEGER;
			update.value = "5";
			update.field = "inte";
			update.op = Base::SO_ADD;

			StorageJson json;
			json.skey = skey;

			json.supdate.push_back(update);

			batchReq.uData.push_back(json);
		}
	}

	BatchDataRsp rsp;

	ret = prx->doBatch(batchReq,  rsp);

	ASSERT_TRUE(ret == 0);
	LOG_CONSOLE_DEBUG << batchReq.sData.size() << ", " << rsp.sRsp.size() << endl;
	ASSERT_TRUE(batchReq.sData.size() == rsp.sRsp.size());
	for(auto k : rsp.sRsp)
	{
		ASSERT_TRUE(k.second == S_OK);
	}

	vector<StorageKey> sRspKey;
	for(auto &data : batchReq.sData)
	{
		sRspKey.push_back(data.skey);
	}

	vector<StorageData> rdata;
	ret = prx->getBatch(options, sRspKey, rdata);
	ASSERT_TRUE(ret == 0);

	ASSERT_TRUE(rdata.size() == sRspKey.size());
	for(auto &data : rdata)
	{
		ASSERT_TRUE(data.ret == 0);
	}

	{
		Base::QueuePopReq popReq;
		popReq.queue = "test";
		popReq.back = false;

		vector<Base::QueueRsp> qRsp;
		ret = prx->get_queue(options, popReq, qRsp);

		ASSERT_TRUE(ret == S_OK);
		ASSERT_TRUE(qRsp[0].data == batchReq.qData[batchReq.qData.size() - 1].data);
	}

	{

		StorageValue value;
		ret = prx->get(options, skey, value);
		ASSERT_TRUE(ret == 0);

		JsonValuePtr pPtr = TC_Json::getValue(string(value.data.data(), value.data.size()));

		JsonValueObjPtr oPtr = JsonValueObjPtr::dynamicCast(pPtr);

		ASSERT_TRUE(oPtr->value.find("str") != oPtr->value.end());
		LOG_CONSOLE_DEBUG << JsonValueStringPtr::dynamicCast(oPtr->value["str"])->value << endl;
		LOG_CONSOLE_DEBUG << JsonValueNumPtr::dynamicCast(oPtr->value["inte"])->lvalue << endl;

		ASSERT_TRUE(JsonValueStringPtr::dynamicCast(oPtr->value["str"])->value == "abcxxxx");
		ASSERT_TRUE(JsonValueNumPtr::dynamicCast(oPtr->value["inte"])->lvalue == 15);

	}

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestList)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	{
		raftTest->startAll();

		raftTest->waitCluster();

		Options options;
		options.leader = true;

		StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

		prx->createTable("test1");
		prx->createTable("test2");
		prx->createTable("test3");

		prx->createQueue("test3");
		prx->createQueue("test4");

		raftTest->stopAll();
	}

	{
		raftTest->startAll();

		StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

		Options options;
		options.leader = true;
		vector<string> tables;
		prx->listTable(options, tables);

		ASSERT_TRUE(tables.size() == 3);

		vector<string> queues;
		prx->listQueue(options, queues);
		ASSERT_TRUE(queues.size() == 1);
		ASSERT_TRUE(queues[0] == "test3");

		raftTest->waitCluster();
		raftTest->stopAll();

	}
}

TEST_F(StorageUnitTest, TestDeleteTable)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	int ret;
	vector<string> tables;

	Options options;
	options.leader = true;
	prx->createTable("test1");
	prx->createTable("test2");

	prx->listTable(options, tables);
	ASSERT_TRUE(tables.size() == 2);

	{
		//测试读
		StorageKey skey;
		skey.table = "test1";
		skey.mkey = "abc";
		skey.ukey = "1";

		ret = prx->has(options, skey);

		ASSERT_TRUE(ret == S_NO_DATA);

		//测试写
		StorageData data;
		data.skey.table = "test1";
		data.skey.mkey 	= "abc";
		data.skey.ukey 	= "1";
		data.svalue.data= {1,1,2,2,3};

		ret = prx->set(data);
		ASSERT_TRUE(ret == 0);

		ret = prx->has(options, skey);

		ASSERT_TRUE(ret == S_OK);

	}

	ret = prx->deleteTable("test1");
	ASSERT_TRUE(ret == S_OK);
	tables.clear();
	prx->listTable(options, tables);
	ASSERT_TRUE(tables.size() == 1);

	{
		//测试读
		StorageKey skey;
		skey.table = "test1";
		skey.mkey = "abc";
		skey.ukey = "1";

		ret = prx->has(options, skey);

		ASSERT_TRUE(ret == S_TABLE_NOT_EXIST);
	}

	raftTest->stopAll();
}


TEST_F(StorageUnitTest, TestDeleteQueue)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	int ret;

	vector<string> queues;
	Options options;
	options.leader = true;
	prx->createQueue("test1");
	prx->createQueue("test2");

	prx->listQueue(options, queues);
	ASSERT_TRUE(queues.size() == 2);


	vector<Base::QueuePushReq> pushReq;
	vector<Base::QueueRsp> rsp;

	QueuePushReq req;
	req.queue = "test1";
	req.back = false;

	req.data.assign(10, 'a');
	pushReq.push_back(req);

	req.data.assign(10, 'b');
	pushReq.push_back(req);

	req.data.assign(10, 'c');
	pushReq.push_back(req);

	ret = prx->push_queue(pushReq);

	ASSERT_TRUE(ret == S_OK);

	Base::QueuePopReq popReq;
	popReq.queue = "test1";
	popReq.back = false;
	popReq.count = 2;

	rsp.clear();
	ret = prx->get_queue(options, popReq, rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.size() == 2);

	ret = prx->deleteQueue("test1");
	ASSERT_TRUE(ret == S_OK);

	ASSERT_TRUE(ret == S_OK);
	queues.clear();
	prx->listQueue(options, queues);
	ASSERT_TRUE(queues.size() == 1);

	rsp.clear();
	ret = prx->get_queue(options, popReq, rsp);

	ASSERT_TRUE(ret == S_QUEUE_NOT_EXIST);

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestTransQueue)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	int ret;

	Options options;
	options.leader = true;
	prx->createQueue("test1");

	vector<Base::QueuePushReq> pushReq;
	vector<Base::QueueRsp> rsp;

	QueuePushReq req;
	req.queue = "test1";
	req.back = true;

	req.data.assign(10, 'a');
	pushReq.push_back(req);

	req.data.assign(10, 'b');
	pushReq.push_back(req);

	req.data.assign(10, 'c');
	pushReq.push_back(req);

	req.data.assign(10, 'd');
	pushReq.push_back(req);

	req.data.assign(10, 'e');
	pushReq.push_back(req);

	ret = prx->push_queue(pushReq);

	ASSERT_TRUE(ret == S_OK);

	QueuePageReq pageReq;
	pageReq.queue = "test1";
	pageReq.index = "";
	pageReq.limit = 2;
	pageReq.forward = true;
	pageReq.include = false;

	{
		ret = prx->transQueue(options, pageReq, rsp);
		ASSERT_TRUE(ret == S_OK);
		ASSERT_TRUE(rsp.size() == pageReq.limit);

		ASSERT_TRUE(rsp[0].data == pushReq[0].data);
		ASSERT_TRUE(rsp[1].data == pushReq[1].data);

		pageReq.index = TC_Common::tostr(rsp[rsp.size() - 1].index);
		rsp.clear();
		ret = prx->transQueue(options, pageReq, rsp);

		ASSERT_TRUE(ret == S_OK);
		ASSERT_TRUE(rsp.size() == 2);
		ASSERT_TRUE(rsp[0].data == pushReq[2].data);
		ASSERT_TRUE(rsp[1].data == pushReq[3].data);

		pageReq.include = true;
		rsp.clear();
		ret = prx->transQueue(options, pageReq, rsp);
		ASSERT_TRUE(ret == S_OK);
		ASSERT_TRUE(rsp.size() == 2);
		ASSERT_TRUE(rsp[0].data == pushReq[1].data);
		ASSERT_TRUE(rsp[1].data == pushReq[2].data);

	}

	{
		pageReq.index = "";
		pageReq.forward = false;
		pageReq.limit = 2;
		pageReq.include = false;

		ret = prx->transQueue(options, pageReq, rsp);
		ASSERT_TRUE(ret == S_OK);
		ASSERT_TRUE(rsp.size() == pageReq.limit);

		ASSERT_TRUE(rsp[0].data == pushReq[pushReq.size()-1].data);
		ASSERT_TRUE(rsp[1].data == pushReq[pushReq.size()-2].data);

		pageReq.index = TC_Common::tostr(rsp[rsp.size()-1].index);
		rsp.clear();
		ret = prx->transQueue(options, pageReq, rsp);

		ASSERT_TRUE(ret == S_OK);
		ASSERT_TRUE(rsp.size() == 2);
		ASSERT_TRUE(rsp[0].data == pushReq[pushReq.size()-3].data);
		ASSERT_TRUE(rsp[1].data == pushReq[pushReq.size()-4].data);

		pageReq.include = true;
		rsp.clear();
		ret = prx->transQueue(options, pageReq, rsp);
		ASSERT_TRUE(ret == S_OK);
		ASSERT_TRUE(rsp.size() == 2);
		ASSERT_TRUE(rsp[0].data == pushReq[pushReq.size()-2].data);
		ASSERT_TRUE(rsp[1].data == pushReq[pushReq.size()-3].data);

	}

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, TestSetQueueData)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	int ret;

	Options options;
	options.leader = true;
	prx->createQueue("test1");

	vector<Base::QueuePushReq> pushReq;
	vector<Base::QueueRsp> rsp;

	QueuePushReq req;
	req.queue = "test1";
	req.expireTime = 0;
	req.back = false;

	req.data.assign(10, 'a');
	pushReq.push_back(req);

	ret = prx->push_queue(pushReq);

	ASSERT_TRUE(ret == S_OK);

	Base::QueuePopReq popReq;
	popReq.queue = "test1";
	popReq.back = false;

	rsp.clear();
	ret = prx->get_queue(options, popReq, rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp[0].data == req.data);

	vector<char> v;
	v.assign(10, 'b');
	vector<Base::QueueRsp> data;
	rsp[0].data = v;

	data.push_back(rsp[0]);

	prx->setQueueData(data);

	vector<QueueIndex> indexReq;
	QueueIndex qi;
	qi.queue = "test1";
	qi.index = rsp[0].index;

	indexReq.push_back(qi);

	rsp.clear();
	ret = prx->getQueueData(options, indexReq, rsp);
	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp[0].data == v);

	raftTest->stopAll();
}

TEST_F(StorageUnitTest, test)
{
	string type = "m5";
	string table = "test";

	TarsOutputStream<BufferWriterString> os;
	os.write(type, 0);
	os.write(table, 1);

	string s = os.getByteBuffer();

	LOG_CONSOLE_DEBUG << s.length() << endl;

	string s1 = "\006\001\065\026\tinstalled";
	LOG_CONSOLE_DEBUG << s1.length() << endl;

	TarsInputStream<> is;
	is.setBuffer(s1.c_str(), s1.length());

	string type1;
	string table1;
	is.read(type1, 0, true);
	is.read(table1, 1, true);

	LOG_CONSOLE_DEBUG << type1 << ", " << table1 << endl;
}

int main(int argc, char** argv)
{
	testing::InitGoogleTest(&argc, argv);

	return RUN_ALL_TESTS();
}

