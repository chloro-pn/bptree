## bptree ##
bptree是一个持久化到磁盘的b+树实现

## feature ##
目前已经实现的特性有：
* 空闲磁盘页的管理
* block lru-cache
* block 通过双向链表连接
* block crc32校验
* 支持Get、Delete、Insert和GetRange等接口
* tool目录下面提供了两个工具，分别用来根据key查找其所在的block index（如果存在的话），以及根据block index打印详细信息等功能

目前正在实现的特性有：
* 完善单元测试 [doing]
* 添加关键路径的日志信息 [doing]
* 实现监控和统计模块，增强bptree的可观测性 [doing]
* 基于WAL机制的数据完整性保证 [todo]
* 并发控制 [todo]

## build ##
在构建之前确保你的编译环境支持c++20标准
git clone下载项目，并在根目录执行bazel build ...即可

## test ##
bazel test ...

## example ##
参考目录example下的example.cc文件，随机生成100000个kv项并插入bptree，然后随机删除其中10000项，最后对比bptree中存储的数据与生成的数据是否一致。

## design and tool ##
* 以下是设计图

<img src="https://github.com/chloro-pn/bptree/blob/master/png/arch.PNG" width = 600 height = 300 alt="" align = center />


* 以下是工具使用截图

<img src="https://github.com/chloro-pn/bptree/blob/master/png/tool.PNG" width = 600 height = 300 alt="" align = center />
