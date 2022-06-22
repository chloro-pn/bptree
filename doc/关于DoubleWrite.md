### 基本概念
参考 https://zhuanlan.zhihu.com/p/268824438 这一文章

#### 本项目中如何处理
为了避免写入磁盘过程中断电或其他意外情况导致的部分写入问题，本项目中的做了如下处理：

* 为每个bptree单独新建一个block_buffer磁盘文件，这个文件的大小就是一个block的大小，只会存储1个block
* 每次在cache将dirty block刷盘回db文件前，首先将该块刷到block_buffer中
* 然后将dirty block写入db
* 在从db读取block时，如果发现block的crc检测失败，则从block_buffer中读出之前写入的块，覆盖掉crc检测失败的块（这个时候block_buffer中的块index和crc检测失败的块的index应该是一致的）

注意，这个过程只在打开数据库，recover的过程中，在执行redo日志从db中导入block时才可能发生。recover结束后如果读到了crc校验失败的块，则应该是bptree内部的bug，或则磁盘文件已经损坏。

update:
* 需要在每次doublewrite写入后进行fsync同步操作（linux下可以在open的时候指定O_SYNC），确保数据在磁盘中持久化后才能开始正式写入操作，同步操作是非常非常消耗性能的
* 所以，考虑将double write buffer放大，支持多个block批量刷盘，均摊fsync开销（也是mysql-innodb的做法）