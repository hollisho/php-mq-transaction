<?php

namespace hollisho\MQTransactionTests\Unit\Producer;

use hollisho\MQTransaction\Database\TransactionHelper;
use hollisho\MQTransaction\MQ\MQConnectorInterface;
use hollisho\MQTransaction\Producer\MessageDispatcher;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class MessageDispatcherTest extends TestCase
{
    private $mqConnector;
    private $transactionHelper;
    private $logger;
    private $messageDispatcher;

    protected function setUp(): void
    {
        $this->mqConnector = $this->createMock(MQConnectorInterface::class);
        $this->transactionHelper = $this->createMock(TransactionHelper::class);
        $this->logger = $this->createMock(LoggerInterface::class);
        
        $this->messageDispatcher = new MessageDispatcher(
            $this->mqConnector,
            $this->transactionHelper,
            $this->logger
        );
    }

    public function testSetBatchSize()
    {
        $batchSize = 200;
        
        $result = $this->messageDispatcher->setBatchSize($batchSize);
        
        $this->assertSame($this->messageDispatcher, $result, '设置批处理大小应返回自身实例');
    }

    public function testSetMaxRetryCount()
    {
        $maxRetryCount = 10;
        
        $result = $this->messageDispatcher->setMaxRetryCount($maxRetryCount);
        
        $this->assertSame($this->messageDispatcher, $result, '设置最大重试次数应返回自身实例');
    }

    public function testDispatchWithSuccessfulSend()
    {
        $pendingMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic-1',
                'data' => json_encode(['key1' => 'value1']),
                'options' => json_encode(['option1' => 'value1']),
                'retry_count' => 0
            ],
            [
                'message_id' => 'test-message-id-2',
                'topic' => 'test-topic-2',
                'data' => json_encode(['key2' => 'value2']),
                'options' => json_encode(['option2' => 'value2']),
                'retry_count' => 1
            ]
        ];
        
        // 设置预期的getPendingMessages调用
        $this->transactionHelper->expects($this->once())
            ->method('getPendingMessages')
            ->with(100) // 默认批处理大小
            ->willReturn($pendingMessages);
        
        // 设置预期的send调用
        $this->mqConnector->expects($this->exactly(count($pendingMessages)))
            ->method('send')
            ->willReturn(true);
        
        // 设置预期的markMessageAsSent调用
        $this->transactionHelper->expects($this->exactly(count($pendingMessages)))
            ->method('markMessageAsSent')
            ->withConsecutive(
                [$pendingMessages[0]['message_id']],
                [$pendingMessages[1]['message_id']]
            );
        
        // 设置预期的日志记录
        $this->logger->expects($this->exactly(count($pendingMessages)))
            ->method('info')
            ->withConsecutive(
                ['Message sent successfully', $this->anything()],
                ['Message sent successfully', $this->anything()]
            );
        
        $sentCount = $this->messageDispatcher->dispatch();
        
        $this->assertEquals(count($pendingMessages), $sentCount, '应返回成功发送的消息数量');
    }

    public function testDispatchWithFailedSend()
    {
        $pendingMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic-1',
                'data' => json_encode(['key1' => 'value1']),
                'options' => json_encode(['option1' => 'value1']),
                'retry_count' => 0
            ]
        ];
        
        // 设置预期的getPendingMessages调用
        $this->transactionHelper->expects($this->once())
            ->method('getPendingMessages')
            ->willReturn($pendingMessages);
        
        // 设置预期的send调用失败
        $this->mqConnector->expects($this->once())
            ->method('send')
            ->willReturn(false);
        
        // 设置预期的incrementRetryCount调用
        $this->transactionHelper->expects($this->once())
            ->method('incrementRetryCount')
            ->with($pendingMessages[0]['message_id']);
        
        // 设置预期的markMessageAsSent调用不会被执行
        $this->transactionHelper->expects($this->never())
            ->method('markMessageAsSent');
        
        // 设置预期的日志记录
        $this->logger->expects($this->once())
            ->method('warning')
            ->with('Failed to send message', $this->anything());
        
        $sentCount = $this->messageDispatcher->dispatch();
        
        $this->assertEquals(0, $sentCount, '失败的发送不应计入发送数量');
    }

    public function testDispatchWithMaxRetryExceeded()
    {
        $pendingMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic-1',
                'data' => json_encode(['key1' => 'value1']),
                'options' => json_encode(['option1' => 'value1']),
                'retry_count' => 5 // 默认最大重试次数为5
            ]
        ];
        
        // 设置预期的getPendingMessages调用
        $this->transactionHelper->expects($this->once())
            ->method('getPendingMessages')
            ->willReturn($pendingMessages);
        
        // 设置预期的send调用失败
        $this->mqConnector->expects($this->once())
            ->method('send')
            ->willReturn(false);
        
        // 设置预期的markMessageAsFailed调用
        $this->transactionHelper->expects($this->once())
            ->method('markMessageAsFailed')
            ->with($pendingMessages[0]['message_id'], 'Max retry count exceeded');
        
        // 设置预期的incrementRetryCount调用不会被执行
        $this->transactionHelper->expects($this->never())
            ->method('incrementRetryCount');
        
        // 设置预期的日志记录
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Max retry count exceeded for message', $this->anything());
        
        $sentCount = $this->messageDispatcher->dispatch();
        
        $this->assertEquals(0, $sentCount, '超过最大重试次数的消息不应计入发送数量');
    }

    public function testDispatchWithException()
    {
        $pendingMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic-1',
                'data' => json_encode(['key1' => 'value1']),
                'options' => json_encode(['option1' => 'value1']),
                'retry_count' => 0
            ]
        ];
        $exception = new \Exception('Test exception');
        
        // 设置预期的getPendingMessages调用
        $this->transactionHelper->expects($this->once())
            ->method('getPendingMessages')
            ->willReturn($pendingMessages);
        
        // 设置预期的send调用抛出异常
        $this->mqConnector->expects($this->once())
            ->method('send')
            ->willThrowException($exception);
        
        // 设置预期的incrementRetryCount调用
        $this->transactionHelper->expects($this->once())
            ->method('incrementRetryCount')
            ->with($pendingMessages[0]['message_id']);
        
        // 设置预期的日志记录
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Exception during message dispatch', $this->anything());
        
        $sentCount = $this->messageDispatcher->dispatch();
        
        $this->assertEquals(0, $sentCount, '异常情况下不应计入发送数量');
    }
}