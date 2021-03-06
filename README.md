## bptree ##
bptree是一个持久化到磁盘的b+树实现，支持Get、Delete、Insert、Update和GetRange操作。通过redo-undo日志+double write机制+check point机制保证单个操作的原子性和持久性。

## feature ##
目前已经实现的特性有：
* 空闲磁盘页的管理
* block lru-cache
* double write机制防止partial write
* 基于redo-undo日志的恢复机制（保证单个操作的原子性和持久性）
* 使用direct-io避免page cache
* check-point机制

## build ##
在构建之前确保你的编译环境支持c++20标准
git clone下载项目，并在根目录执行bazel build ...即可

## doc ##
https://softacheck.com/app/docs/chloro-pn/bptree/

## test ##
bazel test ...

## example ##
参考目录example下的example.cc文件，随机生成4w个kv项并插入bptree，然后随机删除其中1w项，最后对比bptree中存储的数据与生成的数据是否一致。

执行结果参考如下:
```
[2022-05-12 13:43:03.929] [info] create db test.db succ
[2022-05-12 13:43:08.376] [info] insert complete
[2022-05-12 13:43:09.251] [info] insert-get check succ
[2022-05-12 13:43:09.251] [info] print root block's info : 
[2022-05-12 13:43:09.251] [info] -------begin to print block's info-------
[2022-05-12 13:43:09.251] [info] index : 1
[2022-05-12 13:43:09.251] [info] height : 2
[2022-05-12 13:43:09.251] [info] prev : 0, next : 0
[2022-05-12 13:43:09.251] [info] 0 th kv : (next entry index)1 (key)dbqmvvxvag (value)470
[2022-05-12 13:43:09.251] [info] 1 th kv : (next entry index)8 (key)ghcgfcqhzi (value)928
[2022-05-12 13:43:09.251] [info] 2 th kv : (next entry index)4 (key)jpojnfnbzo (value)209
[2022-05-12 13:43:09.251] [info] 3 th kv : (next entry index)7 (key)mxotwhnhyq (value)913
[2022-05-12 13:43:09.251] [info] 4 th kv : (next entry index)2 (key)pxoszezvuq (value)508
[2022-05-12 13:43:09.251] [info] 5 th kv : (next entry index)6 (key)tcvdufaxmh (value)911
[2022-05-12 13:43:09.251] [info] 6 th kv : (next entry index)3 (key)wnbcmekqyg (value)92
[2022-05-12 13:43:09.251] [info] 7 th kv : (next entry index)5 (key)zzzzsxoxwm (value)887
[2022-05-12 13:43:09.251] [info] --------end to print block's info--------
[2022-05-12 13:43:09.251] [info] print cache's info : 
[2022-05-12 13:43:09.251] [info] ---begin to print block_cache's info---
[2022-05-12 13:43:09.251] [info] the length of the list in_use is 0
[2022-05-12 13:43:09.251] [info] the length of the list lru is 1024
[2022-05-12 13:43:09.251] [info] the size of the map cache's 1024
[2022-05-12 13:43:09.251] [info] ----end to print block_cache's info----
[2022-05-12 13:43:09.453] [info] randomly delete 10000 kvs
[2022-05-12 13:43:09.737] [info] range-get the first 1000 kvs and check them
[2022-05-12 13:43:09.737] [info] all check succ
[2022-05-12 13:43:09.737] [info] -----begin super block print-----
[2022-05-12 13:43:09.738] [info] root_index : 1
[2022-05-12 13:43:09.738] [info] key size and value size : 10 20
[2022-05-12 13:43:09.738] [info] free block size : 85
[2022-05-12 13:43:09.738] [info] total block size : 1194
[2022-05-12 13:43:09.738] [info] free_block_size / total_block_size : 0.0711892797319933
[2022-05-12 13:43:09.738] [info] ------end super block print------
```

## design ##
<img src="https://github.com/chloro-pn/bptree/blob/master/png/arch.PNG" width = 600 height = 350 alt="" align = center />

