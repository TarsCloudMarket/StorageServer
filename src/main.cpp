#include <iostream>
#include "StorageServer.h"
#include "servant/QueryF.h"

using namespace tars;

StorageServer g_app;

int main(int argc, char* argv[])
{
	try
	{
//		 TC_Option option;
//		 option.decode(argc, argv);
//
//		 if(option.hasParam("obj"))
//		 {
//			 Communicator communicator;
//
//			 ServantPrx prx= communicator.stringToProxy<ServantPrx>("Hospital.StorageServer.RaftObj@tcp -h hospital-storageserver-2.hospital-storageserver -p 10000");
//
//			 while(true)
//			 {
//				 try
//				 {
//					 prx->tars_ping();
//
//					 LOG_CONSOLE_DEBUG << "succ" << endl;
//				 }
//				 catch (exception &ex)
//				 {
//					 LOG_CONSOLE_DEBUG << "error:" << ex.what() << endl;
//				 }
//
//				 TC_Common::msleep(100);
//			 }
//
//			 exit(0);
//		 }

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