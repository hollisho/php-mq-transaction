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
        
        // 使用反射设置私有属性
        $reflectionClass = new \ReflectionClass(RabbitMQConnector::class);
        
        $channelProperty = $reflectionClass->getProperty('channel');
        $channelProperty->setAccessible(true);
        $channelProperty->setValue($this->connector, $this->channel);
        
        // 设置预期的exchange_declare调用
        $this->channel->expects($this->once())
            ->method('exchange_declare')
            ->with(
                $topic,
                'topic',
                false,
                true,
                false
            )
            ->willReturn(null);  // 确保方法返回null
        
        // 设置预期的basic_publish调用
        $this->channel->expects($this->once())
            ->method('basic_publish')
            ->with(
                $this->callback(function ($message) use ($data, $messageId) {
                    $this->assertInstanceOf(AMQPMessage::class, $message);
                    
                    // 验证消息内容 - 应该是直接编码的数据，而不是包含data和message_id字段的对象
                    $body = json_decode($message->getBody(), true);
                    $this->assertEquals($data, $body, '消息内容应与发送的数据匹配');
                    
                    // 验证消息属性
                    $properties = $message->get_properties();
                    $this->assertEquals($messageId, $properties['message_id'], '消息ID应匹配');
                    $this->assertEquals('application/json', $properties['content_type'], '内容类型应为application/json');
                    $this->assertEquals(AMQPMessage::DELIVERY_MODE_PERSISTENT, $properties['delivery_mode'], '传递模式应为持久化');
                    
                    return true;
                }),
                $topic,
                $topic  // 使用topic作为routing key
            )
            ->willReturn(null);  // 确保方法返回null
        
        // 设置日志记录期望
        $this->logger->expects($this->once())
            ->method('info')
            ->with('Message published', $this->anything())
            ->willReturn(null);
        
        try {
            $result = $this->connector->send($topic, $data, $messageId, $options);
            $this->assertTrue($result, '发送消息应返回true');
        } catch (\Exception $e) {
            $this->fail('发送消息时抛出异常: ' . $e->getMessage());
        }
    }
    
    public function testConsume()
    {
        // 由于consume方法包含无限循环且依赖于私有方法，很难进行完整测试
        // 我们将测试标记为跳过，并提供说明
        $this->markTestSkipped(
            'consume方法包含无限循环且依赖于私有方法，难以进行单元测试。' .
            '建议使用功能测试或集成测试来测试此功能。'
        );
        
        // 另一种选择是只测试消费逻辑的某些方面
        // 例如，我们可以测试当抛出异常时会记录错误
        $topics = ['test-topic'];
        $callback = function() { return true; };
        
        // 设置预期的日志记录
        $this->logger->expects($this->once())
            ->method('error')
            ->with('RabbitMQ consumer error', $this->anything());
        
        // 使用反射执行私有方法
        try {
            $method = new \ReflectionMethod($this->connector, 'getChannel');
            $method->setAccessible(true);
            $method->invoke($this->connector);
            
            // 强制抛出异常以触发日志记录
            $this->connector->consume($topics, $callback);
        } catch (\Exception $e) {
            // 预期会抛出异常，所以我们捕获它
            $this->addToAssertionCount(1);
        }
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