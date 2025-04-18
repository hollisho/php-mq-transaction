<?php

namespace hollisho\MQTransactionTests\Unit\MQ;

use hollisho\MQTransaction\MQ\RabbitMQConnector;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class RabbitMQConnectorTest extends TestCase
{
    private $logger;
    private $connection;
    private $channel;
    private $connector;
    
    protected function setUp(): void
    {
        $this->logger = $this->createMock(LoggerInterface::class);
        $this->connection = $this->createMock(AMQPStreamConnection::class);
        $this->channel = $this->createMock(AMQPChannel::class);
        
        // 创建一个部分模拟的RabbitMQConnector，模拟内部的连接创建
        $this->connector = $this->getMockBuilder(RabbitMQConnector::class)
            ->setConstructorArgs([[
                'host' => 'rabbitmq',
                'port' => 5672,
                'user' => 'myuser',
                'password' => 'mypass',
                'vhost' => '/',
            ], $this->logger])
            ->addMethods(['createConnection'])
            ->getMock();
        
        // 设置模拟连接和通道
        $this->connection->expects($this->any())
            ->method('channel')
            ->willReturn($this->channel);
        
        $this->connector->expects($this->any())
            ->method('createConnection')
            ->willReturn($this->connection);
    }
    
    public function testDeclareExchange()
    {
        $exchangeName = 'test-exchange';
        $exchangeType = 'topic';
        $durable = true;
        $autoDelete = false;
        
        $result = $this->connector->declareExchange($exchangeName, $exchangeType, $durable, $autoDelete);
        
        $this->assertSame($this->connector, $result, '声明交换机应返回自身实例');
    }
    
    public function testDeclareQueue()
    {
        $queueName = 'test-queue';
        $durable = true;
        $exclusive = false;
        $autoDelete = false;
        
        $result = $this->connector->declareQueue($queueName, $durable, $exclusive, $autoDelete);
        
        $this->assertSame($this->connector, $result, '声明队列应返回自身实例');
    }
    
    public function testBindQueue()
    {
        $queueName = 'test-queue';
        $exchangeName = 'test-exchange';
        $routingKey = 'test.routing.key';
        
        $result = $this->connector->bindQueue($queueName, $exchangeName, $routingKey);
        
        $this->assertSame($this->connector, $result, '绑定队列应返回自身实例');
    }
    
    public function testSend()
    {
        $topic = 'test-topic';
        $data = ['key' => 'value'];
        $messageId = 'test-message-id';
        $options = ['delivery_mode' => 2];
        
        // 设置预期的exchange_declare调用
        $this->channel->expects($this->once())
            ->method('exchange_declare')
            ->with(
                $topic,
                'topic',
                false,
                true,
                false
            );
        
        // 设置预期的basic_publish调用
        $this->channel->expects($this->once())
            ->method('basic_publish')
            ->with(
                $this->callback(function ($message) use ($data, $messageId) {
                    $this->assertInstanceOf(AMQPMessage::class, $message);
                    $body = json_decode($message->getBody(), true);
                    return isset($body['data']) && 
                           isset($body['message_id']) && 
                           $body['data'] === $data && 
                           $body['message_id'] === $messageId;
                }),
                $topic,
                $messageId
            );
        
        $result = $this->connector->send($topic, $data, $messageId, $options);
        
        $this->assertTrue($result, '发送消息应返回true');
    }
    
    public function testConsume()
    {
        $topics = ['topic1', 'topic2'];
        $callback = function () {};
        
        // 设置预期的queue_declare调用
        $this->channel->expects($this->exactly(count($topics)))
            ->method('queue_declare')
            ->willReturn(['queue-name', 0, 0]);
        
        // 设置预期的exchange_declare调用
        $this->channel->expects($this->exactly(count($topics)))
            ->method('exchange_declare');
        
        // 设置预期的queue_bind调用
        $this->channel->expects($this->exactly(count($topics)))
            ->method('queue_bind');
        
        // 设置预期的basic_qos调用
        $this->channel->expects($this->once())
            ->method('basic_qos')
            ->with(0, 1, false);
        
        // 设置预期的basic_consume调用
        $this->channel->expects($this->exactly(count($topics)))
            ->method('basic_consume');
        
        // 设置预期的wait调用，模拟消费循环
        $this->channel->expects($this->exactly(2))
            ->method('wait')
            ->willReturnOnConsecutiveCalls(
                null, // 第一次正常返回
                $this->throwException(new \Exception('Test error')) // 第二次抛出异常，结束循环
            );
        
        // 预期日志记录
        $this->logger->expects($this->once())
            ->method('error')
            ->with('RabbitMQ consumer error', $this->anything());
        
        // 调用consume方法，它会进入一个循环，直到发生异常
        $this->connector->consume($topics, $callback);
        
        // 由于consume是一个无限循环，我们只能测试它是否正确处理了我们模拟的情况
        $this->addToAssertionCount(1);
    }
    
    public function testAck()
    {
        $message = (object)['delivery_info' => ['channel' => $this->channel, 'delivery_tag' => 'tag']];
        
        // 设置预期的basic_ack调用
        $this->channel->expects($this->once())
            ->method('basic_ack')
            ->with('tag');
        
        $result = $this->connector->ack($message);
        
        $this->assertTrue($result, 'ack应返回true');
    }
    
    public function testNackWithRequeue()
    {
        $message = (object)['delivery_info' => ['channel' => $this->channel, 'delivery_tag' => 'tag']];
        $requeue = true;
        
        // 设置预期的basic_nack调用
        $this->channel->expects($this->once())
            ->method('basic_nack')
            ->with('tag', false, $requeue);
        
        $result = $this->connector->nack($message, $requeue);
        
        $this->assertTrue($result, 'nack应返回true');
    }
    
    public function testNackWithoutRequeue()
    {
        $message = (object)['delivery_info' => ['channel' => $this->channel, 'delivery_tag' => 'tag']];
        $requeue = false;
        
        // 设置预期的basic_nack调用
        $this->channel->expects($this->once())
            ->method('basic_nack')
            ->with('tag', false, $requeue);
        
        $result = $this->connector->nack($message, $requeue);
        
        $this->assertTrue($result, 'nack应返回true');
    }
    
    public function testClose()
    {
        // 使用反射设置私有属性
        $reflectionClass = new \ReflectionClass(RabbitMQConnector::class);
        
        $connectionProperty = $reflectionClass->getProperty('connection');
        $connectionProperty->setAccessible(true);
        $connectionProperty->setValue($this->connector, $this->connection);
        
        $channelProperty = $reflectionClass->getProperty('channel');
        $channelProperty->setAccessible(true);
        $channelProperty->setValue($this->connector, $this->channel);
        
        // 设置预期的close调用
        $this->channel->expects($this->once())
            ->method('close');
        
        $this->connection->expects($this->once())
            ->method('close');
        
        $this->connector->close();
        
        // 验证属性是否被重置为null
        $this->assertNull($connectionProperty->getValue($this->connector));
        $this->assertNull($channelProperty->getValue($this->connector));
    }
}