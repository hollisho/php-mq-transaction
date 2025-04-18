<?php

namespace hollisho\MQTransactionTests\Unit\Producer;

use hollisho\MQTransaction\Database\TransactionHelper;
use hollisho\MQTransaction\MQ\MQConnectorInterface;
use hollisho\MQTransaction\Producer\TransactionProducer;
use PHPUnit\Framework\TestCase;

class TransactionProducerTest extends TestCase
{
    private $mqConnector;
    private $transactionHelper;
    private $transactionProducer;

    protected function setUp(): void
    {
        $this->mqConnector = $this->createMock(MQConnectorInterface::class);
        $this->transactionHelper = $this->createMock(TransactionHelper::class);
        
        $this->transactionProducer = new TransactionProducer(
            $this->mqConnector,
            $this->transactionHelper
        );
    }

    public function testBeginTransaction()
    {
        $this->transactionHelper->expects($this->once())
            ->method('beginTransaction')
            ->willReturn(true);
        
        $this->transactionProducer->beginTransaction();
        
        // 使用反射检查私有属性
        $reflectionClass = new \ReflectionClass(TransactionProducer::class);
        $inTransactionProperty = $reflectionClass->getProperty('inTransaction');
        $inTransactionProperty->setAccessible(true);
        
        $this->assertTrue($inTransactionProperty->getValue($this->transactionProducer), 'inTransaction属性应设置为true');
    }

    public function testBeginTransactionWhenAlreadyInTransaction()
    {
        // 先设置inTransaction为true
        $reflectionClass = new \ReflectionClass(TransactionProducer::class);
        $inTransactionProperty = $reflectionClass->getProperty('inTransaction');
        $inTransactionProperty->setAccessible(true);
        $inTransactionProperty->setValue($this->transactionProducer, true);
        
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Transaction already started');
        
        $this->transactionProducer->beginTransaction();
    }

    public function testPrepareMessage()
    {
        // 先设置inTransaction为true
        $reflectionClass = new \ReflectionClass(TransactionProducer::class);
        $inTransactionProperty = $reflectionClass->getProperty('inTransaction');
        $inTransactionProperty->setAccessible(true);
        $inTransactionProperty->setValue($this->transactionProducer, true);
        
        $preparedMessagesProperty = $reflectionClass->getProperty('preparedMessages');
        $preparedMessagesProperty->setAccessible(true);
        
        $topic = 'test-topic';
        $data = ['key' => 'value'];
        $options = ['option1' => 'value1'];
        
        $messageId = $this->transactionProducer->prepareMessage($topic, $data, $options);
        
        $this->assertNotEmpty($messageId, '应返回非空的消息ID');
        
        $preparedMessages = $preparedMessagesProperty->getValue($this->transactionProducer);
        $this->assertCount(1, $preparedMessages, '应添加一条预备消息');
        $this->assertEquals($messageId, $preparedMessages[0]['message_id'], '消息ID应匹配');
        $this->assertEquals($topic, $preparedMessages[0]['topic'], '主题应匹配');
        $this->assertEquals($data, $preparedMessages[0]['data'], '数据应匹配');
        $this->assertEquals($options, $preparedMessages[0]['options'], '选项应匹配');
        $this->assertEquals('pending', $preparedMessages[0]['status'], '状态应为pending');
    }

    public function testPrepareMessageWhenNotInTransaction()
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('No active transaction');
        
        $this->transactionProducer->prepareMessage('test-topic', ['key' => 'value']);
    }

    public function testCommit()
    {
        // 先设置inTransaction为true并添加预备消息
        $reflectionClass = new \ReflectionClass(TransactionProducer::class);
        $inTransactionProperty = $reflectionClass->getProperty('inTransaction');
        $inTransactionProperty->setAccessible(true);
        $inTransactionProperty->setValue($this->transactionProducer, true);
        
        $preparedMessagesProperty = $reflectionClass->getProperty('preparedMessages');
        $preparedMessagesProperty->setAccessible(true);
        
        $message = [
            'message_id' => 'test-message-id',
            'topic' => 'test-topic',
            'data' => ['key' => 'value'],
            'options' => [],
            'status' => 'pending',
            'created_at' => date('Y-m-d H:i:s'),
            'retry_count' => 0
        ];
        
        $preparedMessagesProperty->setValue($this->transactionProducer, [$message]);
        
        // 设置预期的saveMessage调用
        $this->transactionHelper->expects($this->once())
            ->method('saveMessage')
            ->with($this->callback(function ($savedMessage) use ($message) {
                return $savedMessage['message_id'] === $message['message_id'] &&
                       $savedMessage['topic'] === $message['topic'] &&
                       $savedMessage['data'] === $message['data'];
            }))
            ->willReturn(true);
        
        // 设置预期的commit调用
        $this->transactionHelper->expects($this->once())
            ->method('commit')
            ->willReturn(true);
        
        $this->transactionProducer->commit();
        
        // 验证状态重置
        $this->assertFalse($inTransactionProperty->getValue($this->transactionProducer), 'inTransaction属性应重置为false');
        $this->assertEmpty($preparedMessagesProperty->getValue($this->transactionProducer), 'preparedMessages应重置为空');
    }

    public function testCommitWhenNotInTransaction()
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('No active transaction');
        
        $this->transactionProducer->commit();
    }

    public function testCommitWithSaveMessageFailure()
    {
        // 先设置inTransaction为true并添加预备消息
        $reflectionClass = new \ReflectionClass(TransactionProducer::class);
        $inTransactionProperty = $reflectionClass->getProperty('inTransaction');
        $inTransactionProperty->setAccessible(true);
        $inTransactionProperty->setValue($this->transactionProducer, true);
        
        $preparedMessagesProperty = $reflectionClass->getProperty('preparedMessages');
        $preparedMessagesProperty->setAccessible(true);
        
        $message = [
            'message_id' => 'test-message-id',
            'topic' => 'test-topic',
            'data' => ['key' => 'value'],
            'options' => [],
            'status' => 'pending',
            'created_at' => date('Y-m-d H:i:s'),
            'retry_count' => 0
        ];
        
        $preparedMessagesProperty->setValue($this->transactionProducer, [$message]);
        
        // 设置预期的saveMessage调用失败
        $this->transactionHelper->expects($this->once())
            ->method('saveMessage')
            ->willReturn(false);
        
        // 设置预期的rollback调用
        $this->transactionHelper->expects($this->once())
            ->method('rollback');
        
        // 设置预期的commit调用不会被执行
        $this->transactionHelper->expects($this->never())
            ->method('commit');
        
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Failed to save message');
        
        $this->transactionProducer->commit();
    }

    public function testRollback()
    {
        // 先设置inTransaction为true并添加预备消息
        $reflectionClass = new \ReflectionClass(TransactionProducer::class);
        $inTransactionProperty = $reflectionClass->getProperty('inTransaction');
        $inTransactionProperty->setAccessible(true);
        $inTransactionProperty->setValue($this->transactionProducer, true);
        
        $preparedMessagesProperty = $reflectionClass->getProperty('preparedMessages');
        $preparedMessagesProperty->setAccessible(true);
        $preparedMessagesProperty->setValue($this->transactionProducer, [['message_id' => 'test-message-id']]);
        
        // 设置预期的rollback调用
        $this->transactionHelper->expects($this->once())
            ->method('rollback')
            ->willReturn(true);
        
        $this->transactionProducer->rollback();
        
        // 验证状态重置
        $this->assertFalse($inTransactionProperty->getValue($this->transactionProducer), 'inTransaction属性应重置为false');
        $this->assertEmpty($preparedMessagesProperty->getValue($this->transactionProducer), 'preparedMessages应重置为空');
    }

    public function testRollbackWhenNotInTransaction()
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('No active transaction');
        
        $this->transactionProducer->rollback();
    }
}