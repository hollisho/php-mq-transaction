<?php

namespace hollisho\MQTransactionTests\Unit\Consumer;

use hollisho\MQTransaction\Compensation\CompensationHandler;
use hollisho\MQTransaction\Consumer\CompensationTrigger;
use hollisho\MQTransaction\Database\IdempotencyHelper;
use hollisho\MQTransaction\Database\TransactionHelper;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;

class CompensationTriggerTest extends TestCase
{
    private $transactionHelper;
    private $idempotencyHelper;
    private $compensationHandler;
    private $logger;
    private $compensationTrigger;

    protected function setUp(): void
    {
        $this->transactionHelper = $this->createMock(TransactionHelper::class);
        $this->idempotencyHelper = $this->createMock(IdempotencyHelper::class);
        $this->compensationHandler = $this->createMock(CompensationHandler::class);
        $this->logger = $this->createMock(LoggerInterface::class);
        
        $this->compensationTrigger = new CompensationTrigger(
            $this->transactionHelper,
            $this->idempotencyHelper,
            $this->compensationHandler,
            $this->logger
        );
    }

    public function testSetBatchSize()
    {
        $batchSize = 100;
        
        $result = $this->compensationTrigger->setBatchSize($batchSize);
        
        $this->assertSame($this->compensationTrigger, $result, '设置批处理大小应返回自身实例');
    }

    public function testCheckProducerCompensationWithSuccessfulCompensation()
    {
        $failedMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic-1'
            ],
            [
                'message_id' => 'test-message-id-2',
                'topic' => 'test-topic-2'
            ]
        ];
        
        // 设置预期的getFailedMessages调用
        $this->transactionHelper->expects($this->once())
            ->method('getFailedMessages')
            ->with(50) // 默认批处理大小
            ->willReturn($failedMessages);
        
        // 设置预期的日志记录
        $this->logger->expects($this->exactly(count($failedMessages)))
            ->method('info')
            ->with('Triggering producer compensation', $this->anything());
        
        // 设置预期的handleProducerFailure调用
        $this->compensationHandler->expects($this->exactly(count($failedMessages)))
            ->method('handleProducerFailure')
            ->willReturn(true);
        
        // 设置预期的markMessageAsCompensated调用
        $this->transactionHelper->expects($this->exactly(count($failedMessages)))
            ->method('markMessageAsCompensated')
            ->withConsecutive(
                [$failedMessages[0]['message_id']],
                [$failedMessages[1]['message_id']]
            );
        
        $processedCount = $this->compensationTrigger->checkProducerCompensation();
        
        $this->assertEquals(count($failedMessages), $processedCount, '应返回成功处理的消息数量');
    }

    public function testCheckProducerCompensationWithFailedCompensation()
    {
        $failedMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic-1'
            ]
        ];
        
        // 设置预期的getFailedMessages调用
        $this->transactionHelper->expects($this->once())
            ->method('getFailedMessages')
            ->willReturn($failedMessages);
        
        // 设置预期的日志记录
        $this->logger->expects($this->once())
            ->method('info')
            ->with('Triggering producer compensation', $this->anything());
        
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Producer compensation failed', $this->anything());
        
        // 设置预期的handleProducerFailure调用
        $this->compensationHandler->expects($this->once())
            ->method('handleProducerFailure')
            ->willReturn(false);
        
        // 设置预期的markMessageAsCompensated调用不会被执行
        $this->transactionHelper->expects($this->never())
            ->method('markMessageAsCompensated');
        
        $processedCount = $this->compensationTrigger->checkProducerCompensation();
        
        $this->assertEquals(0, $processedCount, '失败的补偿不应计入处理数量');
    }

    public function testCheckProducerCompensationWithException()
    {
        $failedMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic-1'
            ]
        ];
        $exception = new \Exception('Test exception');
        
        // 设置预期的getFailedMessages调用
        $this->transactionHelper->expects($this->once())
            ->method('getFailedMessages')
            ->willReturn($failedMessages);
        
        // 设置预期的日志记录
        $this->logger->expects($this->once())
            ->method('info')
            ->with('Triggering producer compensation', $this->anything());
        
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Error during producer compensation', $this->anything());
        
        // 设置预期的handleProducerFailure调用抛出异常
        $this->compensationHandler->expects($this->once())
            ->method('handleProducerFailure')
            ->willThrowException($exception);
        
        // 设置预期的markMessageAsCompensated调用不会被执行
        $this->transactionHelper->expects($this->never())
            ->method('markMessageAsCompensated');
        
        $processedCount = $this->compensationTrigger->checkProducerCompensation();
        
        $this->assertEquals(0, $processedCount, '异常情况下不应计入处理数量');
    }

    public function testCheckConsumerCompensationWithSuccessfulCompensation()
    {
        $failedMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic-1'
            ],
            [
                'message_id' => 'test-message-id-2',
                'topic' => 'test-topic-2'
            ]
        ];
        
        // 设置预期的getFailedConsumptions调用
        $this->idempotencyHelper->expects($this->once())
            ->method('getFailedConsumptions')
            ->with(50) // 默认批处理大小
            ->willReturn($failedMessages);
        
        // 设置预期的日志记录
        $this->logger->expects($this->exactly(count($failedMessages)))
            ->method('info')
            ->with('Triggering consumer compensation', $this->anything());
        
        // 设置预期的handleConsumerFailure调用
        $this->compensationHandler->expects($this->exactly(count($failedMessages)))
            ->method('handleConsumerFailure')
            ->willReturn(true);
        
        // 设置预期的markAsProcessed调用
        $this->idempotencyHelper->expects($this->exactly(count($failedMessages)))
            ->method('markAsProcessed')
            ->withConsecutive(
                [$failedMessages[0]['message_id']],
                [$failedMessages[1]['message_id']]
            );
        
        $processedCount = $this->compensationTrigger->checkConsumerCompensation();
        
        $this->assertEquals(count($failedMessages), $processedCount, '应返回成功处理的消息数量');
    }

    public function testCheckConsumerCompensationWithFailedCompensation()
    {
        $failedMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic-1'
            ]
        ];
        
        // 设置预期的getFailedConsumptions调用
        $this->idempotencyHelper->expects($this->once())
            ->method('getFailedConsumptions')
            ->willReturn($failedMessages);
        
        // 设置预期的日志记录
        $this->logger->expects($this->once())
            ->method('info')
            ->with('Triggering consumer compensation', $this->anything());
        
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Consumer compensation failed', $this->anything());
        
        // 设置预期的handleConsumerFailure调用
        $this->compensationHandler->expects($this->once())
            ->method('handleConsumerFailure')
            ->willReturn(false);
        
        // 设置预期的markAsProcessed调用不会被执行
        $this->idempotencyHelper->expects($this->never())
            ->method('markAsProcessed');
        
        $processedCount = $this->compensationTrigger->checkConsumerCompensation();
        
        $this->assertEquals(0, $processedCount, '失败的补偿不应计入处理数量');
    }

    public function testCheckConsumerCompensationWithException()
    {
        $failedMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic-1'
            ]
        ];
        $exception = new \Exception('Test exception');
        
        // 设置预期的getFailedConsumptions调用
        $this->idempotencyHelper->expects($this->once())
            ->method('getFailedConsumptions')
            ->willReturn($failedMessages);
        
        // 设置预期的日志记录
        $this->logger->expects($this->once())
            ->method('info')
            ->with('Triggering consumer compensation', $this->anything());
        
        $this->logger->expects($this->once())
            ->method('error')
            ->with('Error during consumer compensation', $this->anything());
        
        // 设置预期的handleConsumerFailure调用抛出异常
        $this->compensationHandler->expects($this->once())
            ->method('handleConsumerFailure')
            ->willThrowException($exception);
        
        // 设置预期的markAsProcessed调用不会被执行
        $this->idempotencyHelper->expects($this->never())
            ->method('markAsProcessed');
        
        $processedCount = $this->compensationTrigger->checkConsumerCompensation();
        
        $this->assertEquals(0, $processedCount, '异常情况下不应计入处理数量');
    }
}