todo:
* 完善单元测试 [doing]
* 完善错误处理 [done]
* block的crc校验和lru cache策略，是上条的基础 [done] [update 重构了cache接口，便于recover流程处理]
* 记录统计信息和日志 [doing]
* 提供迭代器 [delete]
* 重构page磁盘管理 （目前的这种位图形式还是不太行，消耗空间太大）[done]
* 处理字节序问题
* block的dirty机制，减少不必要的刷盘操作 [done]
* cache希望block的析构函数中进行资源释放（调用manager进行刷盘），但是需要在析构函数中调用虚函数，因此需要修改cache，提供destructor前注册机制 [done]
* cache目前的实现有bug，Block生命周期管理不明确，需要修改接口 [done]
* 基于WAL的崩溃恢复机制 [done]
* double write机制 [done]
* b+树并发控制 [无期限延迟]
* 日志缓冲区、checkpoint [done]
* lsn机制，减少不必要的redo-undo的执行数量 [todo]
* 恢复机制相关的代码重构和优化 [done]
* 增加解析wal日志的工具 [todo]
* 文件锁、提供只读访问模式和互斥写模式 [doing]
* 支持MassTree的变长key机制 [todo]
* 支持自定义key比较函数的机制 [done]
* 支持linux direct-IO机制 [done]
* 支持redo日志缓冲-现在的实现严格上来说是有问题的，事务提交的commit point实际上就是对应的事务结束日志（以及之前的日志）持久化到磁盘的point，因此如果要
实现事务的持久性，每次事务提交都需要对redo日志进行fsync操作，而fsync是非常重的操作。（innodb提供的innodb_flush_log_at_trx_commit就是用来处理这个问题，
用户可以通过配置在数据安全性和性能两方面进行选择）。反而真正记录数据的block（page）的持久化并没有这些要求，唯一的原则是：在一个block刷盘前，对应的redo日志必须已经持久化了。
* 支持n:1的double write buffer，使得多个block均摊fsync操作的开销 [todo]