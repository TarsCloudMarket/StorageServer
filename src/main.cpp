#include <iostream>
#include "StorageServer.h"
#include "servant/QueryF.h"

using namespace tars;

StorageServer g_app;

int main(int argc, char* argv[])
{
	try
	{
		// TC_Option option;
		// option.decode(argc, argv);

		// if(option.hasParam("obj"))
		// {
		// 	TC_Config conf;
		// 	conf.parseFile(option.getValue("config"));

		// 	Communicator communicator;
		// 	communicator.setProperty(conf);

		// 	QueryFPrx prx= communicator.stringToProxy<QueryFPrx>("tars.tarsregistry.QueryObj");

		// 	vector<tars::EndpointF> activeEp;
		// 	vector<tars::EndpointF> inActiveEp;

		// 	int ret = prx->findObjectById4All(option.getValue("obj"), activeEp, inActiveEp);

		// 	LOG_CONSOLE_DEBUG << "ret:" << ret << endl;
		// 	LOG_CONSOLE_DEBUG << "active---------------------------------------size:" << activeEp.size() << endl;

		// 	for(auto ep : activeEp)
		// 	{
		// 		LOG_CONSOLE_DEBUG << ep.host << ":" << ep.port << endl;
		// 	}

		// 	LOG_CONSOLE_DEBUG << "inactive-------------------------------------size:" << inActiveEp.size() << endl;
		// 	for(auto ep : inActiveEp)
		// 	{
		// 		LOG_CONSOLE_DEBUG << ep.host << ":" << ep.port << endl;
		// 	}

		// 	exit(0);
		// }

		g_app.main(argc, argv);

		g_app.waitForShutdown();
	}
	catch (std::exception& e)
	{
		cerr << "std::exception:" << e.what() << std::endl;
	}
	catch (...)
	{
		cerr << "unknown exception." << std::endl;
	}
	return -1;
}