import json
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

class Kafka78:
    def __init__(self,kafka_server=None  ):
        """初始化 Kafka78 类，设置 Kafka 服务器地址"""
        self.kafka_server = kafka_server
        self.producer = None
        self.consumer = None
    
    async def setup(self):
        """初始化 AIOKafkaProducer"""
        
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_server,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')  # 默认消息格式为 JSON
        )
        await self.producer.start()
        return
    
    async def get_remaining_messages(self, topic: str, group_id: str="testgroup") -> int:
        """
        获取剩余的消息数量
        :param topic: Kafka 主题名称
        :param group_id: Kafka 消费者组 ID
        :return: 剩余消息数量
        """
        # 初始化消费者
        await self.setup_consumer(topic, group_id)

        # 查询当前偏移量和最大偏移量
        remaining_messages= await self.get_offsets(topic)
        #print(f"剩余消息数量: {remaining_messages}")  
        # 关闭消费者
        await self.close_consumer() 
   
        return remaining_messages
    
    async def get_offsets(self, topic):
        """获取还有多少消息未消费"""
        partitions =  self.consumer.partitions_for_topic(topic)
    
        if not partitions:
            print(f"没有找到主题 {topic} 的分区")
            return 0
        #for partition in partitions:
           # 获取当前消费者的分配
        assignment = list(self.consumer.assignment())  # 将 frozenset 转换为列表
        if not assignment:
            print(f"消费者没有被分配到任何分区")
            return 0
        
        remaining_messages = 0  # 用于累积未消费的消息数量
    
        for assigned_partition in assignment:
            # 获取当前消费者的位置（即偏移量）
            positions = await self.consumer.position(assigned_partition)
            
            # 获取最大偏移量
            end_offsets = await self.consumer.end_offsets([assigned_partition])
            
            # 计算该分区未消费的消息数（最大偏移量 - 当前偏移量）
            remaining = end_offsets[assigned_partition] - positions if positions < end_offsets[assigned_partition] else 0
            
            # 累加每个分区的未消费消息数
            remaining_messages += remaining

        return remaining_messages
    
    async def setup_consumer(self, topic,groupid="testgroup"):
        """初始化 AIOKafkaConsumer"""
        self.consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.kafka_server,
            group_id=groupid,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # 消息格式为 JSON
            max_poll_interval_ms=30000,  # 设置为更长的时间（5分钟）
            max_poll_records=1000,  # 设置每次 poll 拉取的最大消息数量
        )
        await self.consumer.start()
    
    async def send(self, data, topic):
        """异步将数据发送到指定的 Kafka topic"""
        if data:
            # 发送数据到指定的 topic
            await self.producer.send(topic, value=data)
    # def __del__(self):
    #     """析构函数（对象销毁时调用）"""
    #     #print("Kafka78 object is being destroyed. Closing producer...")
    #     if self.producer:
    #         asyncio.run(self.producer.stop())  # 关闭生产者（需要手动运行异步代码）

    async def __aenter__(self):
        """进入上下文管理器时，初始化并返回 Kafka78 实例"""
        await self.setup()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器时，关闭 Kafka 生产者"""
        await self.close_producer()
    
    
    async def close_producer(self):
        """关闭 Kafka 生产者"""
        await self.producer.stop()
        return
    
    async def close_consumer(self):
        """关闭 Kafka 消费者"""
        await self.consumer.stop()

# 消费者读取消息的函数
async def consume_messages(kafka78, topic):
    """异步读取 Kafka 消息"""
    await kafka78.setup_consumer(topic)
    print(f"开始消费来自 topic '{topic}' 的消息...")
    
    try:
        # 消费 3 条消息
        async for msg in kafka78.consumer:
            print(f"接收到消息: {msg}")
            #接收到消息: ConsumerRecord(topic='test-topic', partition=0, offset=15, timestamp=1732977945882, timestamp_type=0, key=None, value={'key': 
            #'value1', 'message': 'Hello from message 7'}, checksum=None, serialized_key_size=-1, serialized_value_size=52, headers=())
            
            # 假设我们只消费 3 条消息，之后停止
            # if msg.offset >= 2:  # 假设我们消费完 3 条数据
            #     break
    finally:
        await kafka78.close_consumer()
# 示例用法24-12-01过一阵子看看有没有自动删除
async def main():
    kafka78 = Kafka78(kafka_server='192.168.31.181:30008')  # 替换为你实际的 Kafka 地址

    # 使用上下文管理器初始化生产者并发送数据
    async with kafka78:
        # 发送 3 条数据到 `test-topic`
        data1 = {"key": "value1", "message": "Hello from message 7"}
        data2 = {"key": "value2", "message": "Hello from message 8"}
        data3 = {"key": "value3", "message": "Hello from message 9"}

        await kafka78.send(data1, 'test-topic')
        await kafka78.send(data2, 'test-topic')
        await kafka78.send(data3, 'test-topic')

    # 消费刚才发送的 3 条消息
    await consume_messages(kafka78, 'test-topic')
    return 

# 示例用法
async def main2():
    kafka78 = Kafka78(kafka_server='192.168.31.181:30008')  # 替换为实际的 Kafka 地址

    # 初始化消费者
    await kafka78.get_remaining_messages('test-topic')

    # # 查询当前偏移量和最大偏移量
    # await kafka78.get_offsets('test-topic')

    # # 关闭消费者
    # await kafka78.close_consumer()
    return


# 运行主程序
asyncio.run(main2())