#include <cassert>
#include <iostream>
#include <vector>
#include "gtest/gtest.h"
#include "rafttest/RaftTest.h"
#include "StorageServer.h"
#include "Storage.h"

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

	LOG_CONSOLE_DEBUG << endl;
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
			req.forword = false;

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
			req.forword = false;

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
			req.forword = false;

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
			req.forword = false;

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

int main(int argc, char** argv)
{
	testing::InitGoogleTest(&argc, argv);

	return RUN_ALL_TESTS();
}