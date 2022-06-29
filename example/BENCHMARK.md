### 测试环境 ###
os: Ubuntu 20.04
fs: ext4
disk: hdd 5400 rpm

### 测试对象 ###
bptree（本项目）和leveldb（version：d019e3605f2）

### 测试数据 ###
每条数据的key长10字节，value长100字节，一共100w条数据。
三种读写顺序：
* 完全随机
* 完全顺序
* 部分顺序 (首先随机在0-1000内选择一个数字base_index，接下来1000次读写都在[base_index, base_index + 1000)内进行，这同样是随机的)
注：部分顺序中0-1000的序列通过洗牌算法得到，因此不会重复和漏掉。

两种cache配置：
* 20 MB
* 200 MB
注：leveldb的write buffer 固定4MB，通过调节block_cache调整cache配置。

### 注意 ###
采用pcstat工具观察数据库文件的page-cache，可以发现在这个数据量级下，内核100%缓存了查询数据，因此20MB和200MB用户缓存的测试结果差别不大（特殊情况除外，均有说明）。

每次测试前使用 ```echo 1 > /proc/sys/vm/drop_caches``` 清空page-cache。

### 测试结果 ###
##### read #####
cache 20 MB

| | bptree | leveldb |
| :---: | :---: | :---: |
| 随机读 | 110s | 33s |
| 顺序读 | 3.2s | 12.3s|
| 部分顺序读| 16s | 28s |

完全随机读+cache不足以容纳所有block时会导致频繁的读block，每次都需要重新解析block并进行crc32校验（这里读了91w次block），因此导致110s耗时。
对于顺序读和部分顺序读，bptree的性能约为leveldb的2-4倍。

cache 200 MB
| | bptree | leveldb |
| :---: | :---: | :---: |
| 随机读 | 63s | 32s |
| 顺序读 | 3.0s | 11.8s|
| 部分顺序读| 16s | 28s |

在cache足够大时，bptree的随机读比leveldb还是慢，猜测原因是leveldb的数据结构紧凑，导致随机io较少。

##### write #####
cache 20 MB

| | bptree | leveldb |
| :---: | :---: | :---: |
| 随机写 | too slow | 10s |
| 顺序写 | 74s | 2.7s |
| 部分顺序写 | 86s | 3.2s |

对于随机写，当cache不足时bptree的性能更差，对于顺序写和部分顺序写，leveldb的写性能比bptree快30-40倍，差距很明显。


cache 200 MB
| | bptree | leveldb |
| :---: | :---: | :---: |
| 随机写 | 24s | 6.6s |
| 顺序写 | 31s | 2.8s|
| 部分顺序写 | 30s | 2.8s |

当cache足够时，bptree不需要将block反复刷盘并重新读入，因此性能会好一些，但是由于频繁的block split操作导致开销还是很大， 慢leveldb1个数量级。
leveldb的写受cache的影响较少。

### todo ###

* 对bptree随机读进行优化，尤其是多次读入同一块时反复进行crc32校验，在完全随机读的情况下性能太差。
* bptree的insert和delete比update的开销要大，补充update写的性能测试
* 对于读，测试更大的数据量级，使page-cache失效。此时leveldb的读性能相比于bptree应该会更差（需要更多的磁盘io）