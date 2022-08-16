#include <iostream>
#include "StorageServer.h"
#include "servant/QueryF.h"

using namespace tars;

StorageServer g_app;

int main(int argc, char* argv[])
{
	try
	{
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