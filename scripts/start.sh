#!/bin/bash
PROJECT_BIN_DIR=$1
killall -9 StorageServer
sleep 1s

set -x

rm -rf ${PROJECT_BIN_DIR}/StorageServer/1/data
rm -rf ${PROJECT_BIN_DIR}/StorageServer/2/data
rm -rf ${PROJECT_BIN_DIR}/StorageServer/3/data

nohup ${PROJECT_BIN_DIR}/bin/StorageServer --config=${PROJECT_BIN_DIR}/../src/count-1.conf > StorageServer_1.log 2>&1 &
nohup ${PROJECT_BIN_DIR}/bin/StorageServer --config=${PROJECT_BIN_DIR}/../src/count-2.conf > StorageServer_2.log 2>&1 &
nohup ${PROJECT_BIN_DIR}/bin/StorageServer --config=${PROJECT_BIN_DIR}/../src/count-3.conf > StorageServer_3.log 2>&1 &