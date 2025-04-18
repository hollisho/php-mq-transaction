<?php

namespace hollisho\MQTransactionTests\Unit\MQ;

use hollisho\MQTransaction\MQ\KafkaConnector;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class KafkaConnectorTest extends TestCase
{
    private $logger;
    private $connector;
    
    protected function setUp(): void
    {
        if (!class_exists('\RdKafka\Producer')) {
            $this->markTestSkipped('RdKafka extension not available');
        }
        
        $this->logger = $this->createMock(LoggerInterface::class);
        
        // 创建一个部分模拟的KafkaConnector，模拟内部的Kafka对象
        $this->connector = $this->getMockBuilder(KafkaConnector::class)
            ->setConstructorArgs([[
                'brokers' => 'localhost:9092',
                'group.id' => 'test-group',
            ], $this->logger])
            ->onlyMethods(['createProducer', 'createConsumer', 'createTopic'])
            ->getMock();
    }
    
    public function testSend()
    {
        $topic = 'test-topic';
        $data = ['key' => 'value'];
        $messageId = 'test-message-id';
        $options = [];
        
        // 模拟Kafka Producer和Topic对象
        $mockProducer = $this->createMock('\RdKafka\Producer');
        $mockTopic = $this->createMock('\RdKafka\Topic');
        
        $this->connector->expects($this->once())
            ->method('createProducer')
            ->willReturn($mockProducer);
        
        $this->connector->expects($this->once())
            ->method('createTopic')
            ->with($mockProducer, $topic)
            ->willReturn($mockTopic);
        
        // 设置预期的produce调用
        $mockTopic->expects($this->once())
            ->method('produce')
            ->with(
                RD_KAFKA_PARTITION_UA,
                0,
                $this->callback(function ($payload) use ($data, $messageId) {
                    $decodedPayload = json_decode($payload, true);
                    return isset($decodedPayload['data']) && 
                           isset($decodedPayload['message_id']) && 
                           $decodedPayload['data'] === $data && 
                           $decodedPayload['message_id'] === $messageId;
                }),
                $messageId // key
            );
        
        // 设置预期的poll调用
        $mockProducer->expects($this->once())
            ->method('poll')
            ->with(0);
        
        $result = $this->connector->send($topic, $data, $messageId, $options);
        
        $this->assertTrue($result, '发送消息应返回true');
    }
    
    public function testConsume()
    {
        $topics = ['topic1', 'topic2'];
        $callback = function () {};
        
        // 模拟Kafka Consumer对象
        $mockConsumer = $this->createMock('\RdKafka\KafkaConsumer');
        
        $this->connector->expects($this->once())
            ->method('createConsumer')
            ->willReturn($mockConsumer);
        
        // 设置预期的subscribe调用
        $mockConsumer->expects($this->once())
            ->method('subscribe')
            ->with($topics);
        
        // 模拟消费循环
        $mockConsumer->expects($this->exactly(3))
            ->method('consume')
            ->with(10000)
            ->willReturnOnConsecutiveCalls(
                $this->createMock('\RdKafka\Message'), // 第一次返回消息
                $this->throwException(new \RdKafka\Exception('Test timeout')), // 第二次抛出超时异常
                $this->throwException(new \Exception('Test error')) // 第三次抛出一般异常，结束循环
            );
        
        // 预期日志记录
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Kafka consumer error', $this->anything());
        
        // 调用consume方法，它会进入一个循环，直到发生异常
        $this->connector->consume($topics, $callback);
        
        // 由于consume是一个无限循环，我们只能测试它是否正确处理了我们模拟的情况
        $this->addToAssertionCount(1);
    }
    
    public function testAck()
    {
        $message = $this->createMock('\RdKafka\Message');
        
        $result = $this->connector->ack($message);
        
        $this->assertTrue($result, 'ack应返回true');
    }
    
    public function testNack()
    {
        $message = $this->createMock('\RdKafka\Message');
        
        $result = $this->connector->nack($message, true);
        
        $this->assertTrue($result, 'nack应返回true');
    }
    
    public function testClose()
    {
        // 模拟Kafka Producer和Consumer对象
        $mockProducer = $this->createMock('\RdKafka\Producer');
        $mockConsumer = $this->createMock('\RdKafka\KafkaConsumer');
        
        // 使用反射设置私有属性
        $reflectionClass = new \ReflectionClass(KafkaConnector::class);
        
        $producerProperty = $reflectionClass->getProperty('producer');
        $producerProperty->setAccessible(true);
        $producerProperty->setValue($this->connector, $mockProducer);
        
        $consumerProperty = $reflectionClass->getProperty('consumer');
        $consumerProperty->setAccessible(true);
        $consumerProperty->setValue($this->connector, $mockConsumer);
        
        // 设置预期的close调用
        $mockConsumer->expects($this->once())
            ->method('close');
        
        $this->connector->close();
        
        // 验证属性是否被重置为null
        $this->assertNull($producerProperty->getValue($this->connector));
        $this->assertNull($consumerProperty->getValue($this->connector));
    }
}