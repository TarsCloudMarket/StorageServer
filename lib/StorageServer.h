//
// Created by jarod on 2019-08-08.
//

#ifndef _STORAGE_SERVER_H
#define _STORAGE_SERVER_H

#include "servant/Application.h"
#include "RaftServer.h"

using namespace tars;

class StorageStateMachine;
class StorageImp;
class RaftNode;

class StorageServer : public RaftServer<StorageStateMachine, StorageImp>
{
public:
	StorageServer() {}

	/**
	 * 析构
	 */
	virtual ~StorageServer() {}

	/**
	 * 服务初始化
	 **/
	virtual void initialize();

	/**
	 * 服务销毁
	 **/
	virtual void destroyApp();

};


#endif //_STORAGE_SERVER_H
