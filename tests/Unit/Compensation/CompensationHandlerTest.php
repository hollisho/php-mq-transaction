<?php

namespace hollisho\MQTransactionTests\Unit\Compensation;

use hollisho\MQTransaction\Compensation\CompensationHandler;
use PHPUnit\Framework\TestCase;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

class CompensationHandlerTest extends TestCase
{
    private $container;
    private $logger;
    private $compensationHandler;

    protected function setUp(): void
    {
        $this->container = $this->createMock(ContainerInterface::class);
        $this->logger = $this->createMock(LoggerInterface::class);
        $this->compensationHandler = new CompensationHandler($this->container, $this->logger);
    }

    public function testRegisterProducerHandler()
    {
        $topic = 'test-topic';
        $handler = function () { return true; };
        
        $result = $this->compensationHandler->registerProducerHandler($topic, $handler);
        
        $this->assertSame($this->compensationHandler, $result, '注册生产者补偿处理器应返回自身实例');
        
        // 测试处理器是否正确注册
        $message = ['topic' => $topic];
        $this->assertTrue($this->compensationHandler->handleProducerFailure($message), '处理器应被正确调用');
    }

    public function testRegisterConsumerHandler()
    {
        $topic = 'test-topic';
        $handler = function () { return true; };
        
        $result = $this->compensationHandler->registerConsumerHandler($topic, $handler);
        
        $this->assertSame($this->compensationHandler, $result, '注册消费者补偿处理器应返回自身实例');
        
        // 测试处理器是否正确注册
        $message = ['topic' => $topic];
        $this->assertTrue($this->compensationHandler->handleConsumerFailure($message), '处理器应被正确调用');
    }

    public function testHandleProducerFailureWithMissingTopic()
    {
        $message = ['data' => 'test-data']; // 缺少topic字段
        
        $this->logger->expects($this->once())
            ->method('warning')
            ->with('No producer compensation handler for topic', ['topic' => null]);
        
        $result = $this->compensationHandler->handleProducerFailure($message);
        
        $this->assertFalse($result, '缺少主题的消息处理应返回false');
    }

    public function testHandleProducerFailureWithUnregisteredTopic()
    {
        $message = ['topic' => 'unregistered-topic'];
        
        $this->logger->expects($this->once())
            ->method('warning')
            ->with('No producer compensation handler for topic', ['topic' => 'unregistered-topic']);
        
        $result = $this->compensationHandler->handleProducerFailure($message);
        
        $this->assertFalse($result, '未注册主题的消息处理应返回false');
    }

    public function testHandleProducerFailureWithException()
    {
        $topic = 'test-topic';
        $messageId = 'test-message-id';
        $message = ['topic' => $topic, 'message_id' => $messageId];
        $exception = new \Exception('Test exception');
        
        $handler = function () use ($exception) {
            throw $exception;
        };
        
        $this->compensationHandler->registerProducerHandler($topic, $handler);
        
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Producer compensation failed', [
                'message_id' => $messageId,
                'error' => $exception->getMessage()
            ]);
        
        $result = $this->compensationHandler->handleProducerFailure($message);
        
        $this->assertFalse($result, '处理器抛出异常时应返回false');
    }

    public function testHandleConsumerFailureWithMissingTopic()
    {
        $message = ['data' => 'test-data']; // 缺少topic字段
        
        $this->logger->expects($this->once())
            ->method('warning')
            ->with('No consumer compensation handler for topic', ['topic' => null]);
        
        $result = $this->compensationHandler->handleConsumerFailure($message);
        
        $this->assertFalse($result, '缺少主题的消息处理应返回false');
    }

    public function testHandleConsumerFailureWithUnregisteredTopic()
    {
        $message = ['topic' => 'unregistered-topic'];
        
        $this->logger->expects($this->once())
            ->method('warning')
            ->with('No consumer compensation handler for topic', ['topic' => 'unregistered-topic']);
        
        $result = $this->compensationHandler->handleConsumerFailure($message);
        
        $this->assertFalse($result, '未注册主题的消息处理应返回false');
    }

    public function testHandleConsumerFailureWithException()
    {
        $topic = 'test-topic';
        $messageId = 'test-message-id';
        $message = ['topic' => $topic, 'message_id' => $messageId];
        $exception = new \Exception('Test exception');
        
        $handler = function () use ($exception) {
            throw $exception;
        };
        
        $this->compensationHandler->registerConsumerHandler($topic, $handler);
        
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Consumer compensation failed', [
                'message_id' => $messageId,
                'error' => $exception->getMessage()
            ]);
        
        $result = $this->compensationHandler->handleConsumerFailure($message);
        
        $this->assertFalse($result, '处理器抛出异常时应返回false');
    }

    public function testResolveHandlerWithCallable()
    {
        $handler = function ($message) {
            return $message['success'];
        };
        
        $message = ['success' => true];
        
        // 使用反射调用私有方法
        $reflectionClass = new \ReflectionClass(CompensationHandler::class);
        $reflectionMethod = $reflectionClass->getMethod('resolveHandler');
        $reflectionMethod->setAccessible(true);
        
        $resolvedHandler = $reflectionMethod->invoke($this->compensationHandler, $handler);
        
        $this->assertTrue(is_callable($resolvedHandler), '解析的处理器应是可调用的');
        $this->assertTrue($resolvedHandler($message), '解析的处理器应正确执行');
    }

    public function testResolveHandlerWithServiceId()
    {
        $serviceId = 'test.service.id';
        $handler = function ($message) {
            return $message['success'];
        };
        $message = ['success' => true];
        
        $this->container->expects($this->once())
            ->method('get')
            ->with($serviceId)
            ->willReturn($handler);
        
        // 使用反射调用私有方法
        $reflectionClass = new \ReflectionClass(CompensationHandler::class);
        $reflectionMethod = $reflectionClass->getMethod('resolveHandler');
        $reflectionMethod->setAccessible(true);
        
        $resolvedHandler = $reflectionMethod->invoke($this->compensationHandler, $serviceId);
        
        $this->assertTrue(is_callable($resolvedHandler), '解析的服务处理器应是可调用的');
        $this->assertTrue($resolvedHandler($message), '解析的服务处理器应正确执行');
    }
}