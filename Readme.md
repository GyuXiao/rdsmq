## rdsmq
本项目是基于 Redis Streams 实现的简易消息队列客户端 sdk，主要支持的功能有：
- 发布/订阅模式
- 数据可持久化
- 支持消费端 ack 机制
- 支持消息缓存

### 架构
主要分为3个部分：
- redis client：封装了 Redis Streams 的主要相关指令，包括 XADD，XREADGROUP，XACK；
- producer：生产者，通过 XADD 指令实现消息的生产和投递；
- consumer：消费者，通过 XREADGROUP 指令实现消息的消费，通过 XACK 指令实现消息的确认。

![整体架构图](https://gyu-pic-bucket.oss-cn-shenzhen.aliyuncs.com/20231023113453.png)

### 项目重点

本项目的重点代码主要在于 consumer 的消费消息的流程，一图概况如下：

![](https://gyu-pic-bucket.oss-cn-shenzhen.aliyuncs.com/20231023114331.png)

### 优劣分析

#### 优势：

相对轻量化，相比于传统 mq 组件有着更低的使用和运维成本。

#### 劣势

1，redis streams 在存储介质上需要使用内存，因此消息存储容量相对有限；  
2，且同一个 topic 的数据由于对应为同一个 key，因此会被分发到相同节点，无法实现数据的纵向分治，因此不具备类似于 kafka 纵向分区以提高并发度的能力；  
3，基于 redis 实现的 mq 一定是存在消息丢失的风险的. 尽管在生产端和消费端，producer/consumer 在和 mq 交互时可以通过 ack 机制保证在交互环节不出现消息的丢失，然而在 redis 本身存储消息数据的环节就可能存在数据丢失问题，原因在于：
- redis 数据基于内存存储：哪怕通过最严格 aof 等级设置，由于持久化流程本身是异步执行的，也无法保证数据绝对不丢失；
- redis 为保证可用性，会在一定程度上牺牲数据一致性. 在主从复制时，采用的是异步流程，倘若主节点宕机，从节点的数据可能存在滞后，这样在主从切换时消息就可能丢失。