
## v1.1.1 20220815

### cn
- 增加listTable/listQueue 
### en
- add listTable/listQueue

## v1.1.0 20220802

### cn
- 增加了队列模式
- 增加全局遍历(不指定mkey)
- 增加批量写操作(table/queue: doBatch)
### en
- add queue mode
- add trans global
- add batch write(table/queue: doBatch)

## v1.0.2 20220725

### cn
- storage.get/set 支持 ukey是空的情况
- add storage.trans
### en
- storage.get/set support ukey is empty
- add storage.trans


## v1.0.1 20220722

### cn
- 修复参数名错误, forword -> forward
- StorageKey table也作为key
### en
- Fix parameter error: forword -> forward
- StorageKey table as key

## v1.0.0 20220417

### cn
- 第一个版本, 完成基本功能, 具体参看README.md
- 如果数据是json格式, 支持字段级别的更新, 注意只支持number/string/bool/array类型
- 针对json字段支持replace, add, sub, reverse, append操作

### en
- In the first version, the raft parameter can be configured
- If the data is in JSON format, field level update is supported. Note that only number/string/bool/array types are supported
- Support replace, add, sub, reverse, append on JSON field
 


