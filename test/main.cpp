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
	ASSERT_TRUE(ret == S_ERROR);
	for(auto k : rsp)
	{
		ASSERT_TRUE(k.second == S_TABLE_NOT_EXIST);
	}

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
		ASSERT_TRUE(ret == S_ERROR);
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
		ASSERT_TRUE(ret == S_ERROR);
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
			LOG_CONSOLE_DEBUG << data.size() << endl;

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

			LOG_CONSOLE_DEBUG << data.size() << endl;
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

	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	prx->createTable("test_json");

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

	ret = prx->set(data);
	ASSERT_TRUE(ret == 0);

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

	Base::QueueReq req;
	Base::QueueRsp rsp;

	req.queue = "test";
	req.data.assign(10, 'a');
	ret = prx->push_back(req);

	ASSERT_TRUE(ret == S_OK);

	Base::Options options;
	options.leader = true;
	ret = prx->get_back(options, "test", rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.data == req.data);

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

	Base::QueueReq req;
	Base::QueueRsp rsp;

	Base::Options options;
	options.leader = true;
	ret = prx->get_front(options, "test", rsp);

	ASSERT_TRUE(ret == S_NO_DATA);

	req.queue = "test";
	req.data.assign(10, 'a');
	ret = prx->push_front(req);

	ASSERT_TRUE(ret == S_OK);

	options.leader = true;
	ret = prx->get_front(options, "test", rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.data == req.data);

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

	Base::QueueReq req;
	Base::QueueRsp rsp;

	Base::Options options;
	options.leader = true;

	req.queue = "test";
	req.data.assign(10, 'a');
	ret = prx->push_front(req);

	ASSERT_TRUE(ret == S_OK);

	options.leader = true;
	ret = prx->get_front(options, "test", rsp);

	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.data == req.data);

	LOG_CONSOLE_DEBUG << "get data:" << rsp.index << endl;
	ret = prx->getData(options, "test", rsp.index, rsp);
	LOG_CONSOLE_DEBUG << ret << endl;
	ASSERT_TRUE(ret == S_OK);
	ASSERT_TRUE(rsp.data == req.data);

	LOG_CONSOLE_DEBUG << "delete data:" << rsp.index << endl;
	ret = prx->deleteData("test", rsp.index);
	ASSERT_TRUE(ret == S_OK);
	ret = prx->get_front(options, "test", rsp);

	ASSERT_TRUE(ret == S_NO_DATA);
	raftTest->stopAll();
}



TEST_F(StorageUnitTest, TestQueueClearQueue)
{
	auto raftTest = std::make_shared<RaftTest<StorageServer>>();
	raftTest->initialize("Base", "StorageServer", "StorageObj", "storage-log", 22000, 32000);
	raftTest->createServers(3);

	raftTest->startAll();

	raftTest->waitCluster();

	int ret;
	StoragePrx prx = raftTest->get(0)->node()->getBussLeaderPrx<StoragePrx>();

	prx->createQueue("test");

	Base::QueueReq req;
	Base::QueueRsp rsp;
	req.queue = "test";
	req.data.assign(10, 'a');
	ret = prx->push_front(req);
	ASSERT_TRUE(ret == S_OK);

	ret = prx->clearQueue("test");
	LOG_CONSOLE_DEBUG << ret << endl;
	ASSERT_TRUE(ret == S_OK);

	Base::Options options;
	options.leader = true;
	ret = prx->get_front(options, "test", rsp);

	ASSERT_TRUE(ret == S_NO_DATA);

	raftTest->stopAll();
}

int main(int argc, char** argv)
{
	testing::InitGoogleTest(&argc, argv);

	return RUN_ALL_TESTS();
}

