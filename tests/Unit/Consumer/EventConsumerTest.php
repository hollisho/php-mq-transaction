<?php

namespace hollisho\MQTransactionTests\Unit\Consumer;

use hollisho\MQTransaction\Consumer\EventConsumer;
use hollisho\MQTransaction\Database\IdempotencyHelper;
use hollisho\MQTransaction\MQ\MQConnectorInterface;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class EventConsumerTest extends TestCase
{
    private $mqConnector;
    private $idempotencyHelper;
    private $logger;
    private $eventConsumer;

    protected function setUp(): void
    {
        $this->mqConnector = $this->createMock(MQConnectorInterface::class);
        $this->idempotencyHelper = $this->createMock(IdempotencyHelper::class);
        $this->logger = $this->createMock(LoggerInterface::class);
        
        $this->eventConsumer = new EventConsumer(
            $this->mqConnector,
            $this->idempotencyHelper,
            $this->logger
        );
    }

    public function testRegisterHandler()
    {
        $topic = 'test-topic';
        $handler = function () { return true; };
        
        $result = $this->eventConsumer->registerHandler($topic, $handler);
        
        $this->assertSame($this->eventConsumer, $result, '注册处理器应返回自身实例');
    }

    public function testProcessMessageWithInvalidFormat()
    {
        $message = ['data' => 'test-data']; // 缺少必要字段
        
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Invalid message format', ['message' => $message]);
        
        $result = $this->eventConsumer->processMessage($message);
        
        $this->assertFalse($result, '无效格式的消息处理应返回false');
    }

    public function testProcessMessageAlreadyProcessed()
    {
        $messageId = 'test-message-id';
        $topic = 'test-topic';
        $message = ['message_id' => $messageId, 'topic' => $topic];
        
        $this->idempotencyHelper->expects($this->once())
            ->method('isProcessed')
            ->with($messageId)
            ->willReturn(true);
        
        $this->logger->expects($this->once())
            ->method('info')
            ->with('Message already processed, skipping', ['message_id' => $messageId]);
        
        $result = $this->eventConsumer->processMessage($message);
        
        $this->assertTrue($result, '已处理的消息应返回true');
    }

    public function testProcessMessageWithNoHandler()
    {
        $messageId = 'test-message-id';
        $topic = 'test-topic';
        $message = ['message_id' => $messageId, 'topic' => $topic];
        
        $this->idempotencyHelper->expects($this->once())
            ->method('isProcessed')
            ->with($messageId)
            ->willReturn(false);
        
        $this->logger->expects($this->once())
            ->method('warning')
            ->with('No handler registered for topic', ['topic' => $topic]);
        
        $result = $this->eventConsumer->processMessage($message);
        
        $this->assertFalse($result, '无处理器的消息处理应返回false');
    }

    public function testProcessMessageSuccessfully()
    {
        $messageId = 'test-message-id';
        $topic = 'test-topic';
        $message = ['message_id' => $messageId, 'topic' => $topic, 'data' => ['key' => 'value']];
        
        // 注册处理器
        $handlerCalled = false;
        $handler = function ($data) use (&$handlerCalled, $message) {
            $handlerCalled = true;
            $this->assertEquals($message['data'], $data);
            return true;
        };
        
        $this->eventConsumer->registerHandler($topic, $handler);
        
        $this->idempotencyHelper->expects($this->once())
            ->method('isProcessed')
            ->with($messageId)
            ->willReturn(false);
        
        $this->idempotencyHelper->expects($this->once())
            ->method('markAsProcessing')
            ->with($messageId);
        
        $this->idempotencyHelper->expects($this->once())
            ->method('markAsProcessed')
            ->with($messageId);
        
        $this->logger->expects($this->once())
            ->method('info')
            ->with('Message processed successfully', ['message_id' => $messageId, 'topic' => $topic]);
        
        $result = $this->eventConsumer->processMessage($message);
        
        $this->assertTrue($result, '成功处理的消息应返回true');
        $this->assertTrue($handlerCalled, '处理器应被调用');
    }

    public function testProcessMessageWithHandlerFailure()
    {
        $messageId = 'test-message-id';
        $topic = 'test-topic';
        $message = ['message_id' => $messageId, 'topic' => $topic, 'data' => ['key' => 'value']];
        
        // 注册失败的处理器
        $handler = function () {
            return false;
        };
        
        $this->eventConsumer->registerHandler($topic, $handler);
        
        $this->idempotencyHelper->expects($this->once())
            ->method('isProcessed')
            ->with($messageId)
            ->willReturn(false);
        
        $this->idempotencyHelper->expects($this->once())
            ->method('markAsProcessing')
            ->with($messageId);
        
        $this->idempotencyHelper->expects($this->once())
            ->method('markAsFailed')
            ->with($messageId, 'Handler returned false');
        
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Message processing failed', $this->anything());
        
        $result = $this->eventConsumer->processMessage($message);
        
        $this->assertFalse($result, '处理失败的消息应返回false');
    }

    public function testProcessMessageWithHandlerException()
    {
        $messageId = 'test-message-id';
        $topic = 'test-topic';
        $message = ['message_id' => $messageId, 'topic' => $topic, 'data' => ['key' => 'value']];
        $exception = new \Exception('Test exception');
        
        // 注册抛出异常的处理器
        $handler = function () use ($exception) {
            throw $exception;
        };
        
        $this->eventConsumer->registerHandler($topic, $handler);
        
        $this->idempotencyHelper->expects($this->once())
            ->method('isProcessed')
            ->with($messageId)
            ->willReturn(false);
        
        $this->idempotencyHelper->expects($this->once())
            ->method('markAsProcessing')
            ->with($messageId);
        
        $this->idempotencyHelper->expects($this->once())
            ->method('markAsFailed')
            ->with($messageId, $exception->getMessage());
        
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Exception during message processing', $this->anything());
        
        $result = $this->eventConsumer->processMessage($message);
        
        $this->assertFalse($result, '处理异常的消息应返回false');
    }

    public function testStartConsumingWithTopics()
    {
        $topics = ['topic1', 'topic2'];
        
        $this->mqConnector->expects($this->once())
            ->method('consume')
            ->with($topics, $this->callback(function ($callback) {
                return is_callable($callback);
            }));
        
        $this->eventConsumer->startConsuming($topics);
        
        // 由于startConsuming会调用consume方法，我们只能测试它是否正确传递了参数
        $this->addToAssertionCount(1);
    }
}