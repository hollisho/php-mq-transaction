<?php

namespace hollisho\MQTransactionTests\Unit\Database;

use hollisho\MQTransaction\Database\TransactionHelper;
use PHPUnit\Framework\TestCase;

class TransactionHelperTest extends TestCase
{
    private $pdo;
    private $transactionHelper;
    private $tableName = 'test_mq_messages';

    protected function setUp(): void
    {
        $this->pdo = $this->createMock(\PDO::class);
        $this->transactionHelper = new TransactionHelper($this->pdo, $this->tableName);
    }

    public function testBeginTransaction()
    {
        $this->pdo->expects($this->once())
            ->method('beginTransaction')
            ->willReturn(true);

        $result = $this->transactionHelper->beginTransaction();

        $this->assertTrue($result, '开始事务应返回true');
    }

    public function testCommit()
    {
        $this->pdo->expects($this->once())
            ->method('commit')
            ->willReturn(true);

        $result = $this->transactionHelper->commit();

        $this->assertTrue($result, '提交事务应返回true');
    }

    public function testRollbackWhenInTransaction()
    {
        $this->pdo->expects($this->once())
            ->method('inTransaction')
            ->willReturn(true);

        $this->pdo->expects($this->once())
            ->method('rollBack')
            ->willReturn(true);

        $result = $this->transactionHelper->rollback();

        $this->assertTrue($result, '回滚事务应返回true');
    }

    public function testRollbackWhenNotInTransaction()
    {
        $this->pdo->expects($this->once())
            ->method('inTransaction')
            ->willReturn(false);

        $this->pdo->expects($this->never())
            ->method('rollBack');

        $result = $this->transactionHelper->rollback();

        $this->assertTrue($result, '不在事务中回滚应返回true');
    }

    public function testSaveMessage()
    {
        $message = [
            'message_id' => 'test-message-id',
            'topic' => 'test-topic',
            'data' => ['key' => 'value'],
            'options' => ['option1' => 'value1'],
            'status' => 'pending',
            'created_at' => '2023-01-01 00:00:00',
            'retry_count' => 0
        ];

        $stmt = $this->createMock(\PDOStatement::class);

        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains("INSERT INTO {$this->tableName}"))
            ->willReturn($stmt);

        $stmt->expects($this->once())
            ->method('execute')
            ->with($this->callback(function ($params) use ($message) {
                return $params['message_id'] === $message['message_id'] &&
                       $params['topic'] === $message['topic'] &&
                       $params['data'] === json_encode($message['data']) &&
                       $params['options'] === json_encode($message['options']) &&
                       $params['status'] === $message['status'] &&
                       $params['created_at'] === $message['created_at'] &&
                       $params['updated_at'] === $message['created_at'] &&
                       $params['retry_count'] === $message['retry_count'];
            }))
            ->willReturn(true);

        $result = $this->transactionHelper->saveMessage($message);

        $this->assertTrue($result, '保存消息应返回true');
    }

    public function testGetPendingMessages()
    {
        $limit = 10;
        $expectedMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic',
                'data' => '{"key":"value1"}',
                'status' => 'pending'
            ],
            [
                'message_id' => 'test-message-id-2',
                'topic' => 'test-topic',
                'data' => '{"key":"value2"}',
                'status' => 'pending'
            ]
        ];

        $stmt = $this->createMock(\PDOStatement::class);

        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains("SELECT * FROM {$this->tableName}"))
            ->willReturn($stmt);

        $stmt->expects($this->once())
            ->method('execute')
            ->with(['limit' => $limit]);

        $stmt->expects($this->once())
            ->method('fetchAll')
            ->with(\PDO::FETCH_ASSOC)
            ->willReturn($expectedMessages);

        $result = $this->transactionHelper->getPendingMessages($limit);

        $this->assertEquals($expectedMessages, $result, '应返回待处理的消息列表');
    }

    public function testGetFailedMessages()
    {
        $limit = 10;
        $expectedMessages = [
            [
                'message_id' => 'test-message-id-1',
                'topic' => 'test-topic',
                'data' => '{"key":"value1"}',
                'status' => 'failed'
            ],
            [
                'message_id' => 'test-message-id-2',
                'topic' => 'test-topic',
                'data' => '{"key":"value2"}',
                'status' => 'failed'
            ]
        ];

        $stmt = $this->createMock(\PDOStatement::class);

        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains("SELECT * FROM {$this->tableName}"))
            ->willReturn($stmt);

        $stmt->expects($this->once())
            ->method('execute')
            ->with(['limit' => $limit]);

        $stmt->expects($this->once())
            ->method('fetchAll')
            ->with(\PDO::FETCH_ASSOC)
            ->willReturn($expectedMessages);

        $result = $this->transactionHelper->getFailedMessages($limit);

        $this->assertEquals($expectedMessages, $result, '应返回失败的消息列表');
    }

    public function testMarkMessageAsSent()
    {
        $messageId = 'test-message-id';
        $stmt = $this->createMock(\PDOStatement::class);

        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains("UPDATE {$this->tableName} SET status = 'sent'"))
            ->willReturn($stmt);

        $stmt->expects($this->once())
            ->method('execute')
            ->with($this->callback(function ($params) use ($messageId) {
                return $params['message_id'] === $messageId &&
                       isset($params['updated_at']);
            }))
            ->willReturn(true);

        $result = $this->transactionHelper->markMessageAsSent($messageId);

        $this->assertTrue($result, '标记消息为已发送应返回true');
    }

    public function testMarkMessageAsFailed()
    {
        $messageId = 'test-message-id';
        $errorMessage = 'Test error message';
        $stmt = $this->createMock(\PDOStatement::class);

        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains("UPDATE {$this->tableName} SET status = 'failed'"))
            ->willReturn($stmt);

        $stmt->expects($this->once())
            ->method('execute')
            ->with($this->callback(function ($params) use ($messageId, $errorMessage) {
                return $params['message_id'] === $messageId &&
                       $params['error'] === $errorMessage &&
                       isset($params['updated_at']);
            }))
            ->willReturn(true);

        $result = $this->transactionHelper->markMessageAsFailed($messageId, $errorMessage);

        $this->assertTrue($result, '标记消息为失败应返回true');
    }

    public function testMarkMessageAsCompensated()
    {
        $messageId = 'test-message-id';
        $stmt = $this->createMock(\PDOStatement::class);

        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains("UPDATE {$this->tableName} SET status = 'compensated'"))
            ->willReturn($stmt);

        $stmt->expects($this->once())
            ->method('execute')
            ->with($this->callback(function ($params) use ($messageId) {
                return $params['message_id'] === $messageId &&
                       isset($params['updated_at']);
            }))
            ->willReturn(true);

        $result = $this->transactionHelper->markMessageAsCompensated($messageId);

        $this->assertTrue($result, '标记消息为已补偿应返回true');
    }

    public function testIncrementRetryCount()
    {
        $messageId = 'test-message-id';
        $stmt = $this->createMock(\PDOStatement::class);

        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with($this->stringContains("UPDATE {$this->tableName} SET retry_count = retry_count + 1"))
            ->willReturn($stmt);

        $stmt->expects($this->once())
            ->method('execute')
            ->with($this->callback(function ($params) use ($messageId) {
                return $params['message_id'] === $messageId &&
                       isset($params['updated_at']);
            }))
            ->willReturn(true);

        $result = $this->transactionHelper->incrementRetryCount($messageId);

        $this->assertTrue($result, '增加重试次数应返回true');
    }

    public function testDeleteMessage()
    {
        $messageId = 'test-message-id';
        $stmt = $this->createMock(\PDOStatement::class);

        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with("DELETE FROM {$this->tableName} WHERE message_id = :message_id")
            ->willReturn($stmt);

        $stmt->expects($this->once())
            ->method('execute')
            ->with(['message_id' => $messageId])
            ->willReturn(true);

        $result = $this->transactionHelper->deleteMessage($messageId);

        $this->assertTrue($result, '删除消息应返回true');
    }
}