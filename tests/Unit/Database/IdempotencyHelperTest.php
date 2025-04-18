<?php

namespace hollisho\MQTransactionTests\Unit\Database;

use hollisho\MQTransaction\Database\IdempotencyHelper;
use PHPUnit\Framework\TestCase;

class IdempotencyHelperTest extends TestCase
{
    private $pdo;
    private $idempotencyHelper;
    private $tableName = 'test_mq_consumption_records';

    protected function setUp(): void
    {
        $this->pdo = $this->createMock(\PDO::class);
        $this->idempotencyHelper = new IdempotencyHelper($this->pdo, $this->tableName);
    }

    public function testIsProcessedReturnsTrueForProcessedMessage()
    {
        $messageId = 'test-message-id';
        $stmt = $this->createMock(\PDOStatement::class);
        
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with("SELECT status FROM {$this->tableName} WHERE message_id = :message_id")
            ->willReturn($stmt);
        
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['message_id' => $messageId]);
        
        $stmt->expects($this->once())
            ->method('fetch')
            ->with(\PDO::FETCH_ASSOC)
            ->willReturn(['status' => 'processed']);
        
        $result = $this->idempotencyHelper->isProcessed($messageId);
        
        $this->assertTrue($result, '已处理的消息应返回true');
    }

    public function testIsProcessedReturnsFalseForUnprocessedMessage()
    {
        $messageId = 'test-message-id';
        $stmt = $this->createMock(\PDOStatement::class);
        
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with("SELECT status FROM {$this->tableName} WHERE message_id = :message_id")
            ->willReturn($stmt);
        
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['message_id' => $messageId]);
        
        $stmt->expects($this->once())
            ->method('fetch')
            ->with(\PDO::FETCH_ASSOC)
            ->willReturn(['status' => 'processing']);
        
        $result = $this->idempotencyHelper->isProcessed($messageId);
        
        $this->assertFalse($result, '处理中的消息应返回false');
    }

    public function testIsProcessedReturnsFalseForNonExistentMessage()
    {
        $messageId = 'test-message-id';
        $stmt = $this->createMock(\PDOStatement::class);
        
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with("SELECT status FROM {$this->tableName} WHERE message_id = :message_id")
            ->willReturn($stmt);
        
        $stmt->expects($this->once())
            ->method('execute')
            ->with(['message_id' => $messageId]);
        
        $stmt->expects($this->once())
            ->method('fetch')
            ->with(\PDO::FETCH_ASSOC)
            ->willReturn(false);
        
        $result = $this->idempotencyHelper->isProcessed($messageId);
        
        $this->assertFalse($result, '不存在的消息应返回false');
    }

    public function testMarkAsProcessingForNewRecord()
    {
        $messageId = 'test-message-id';
        $topic = 'test-topic';
        $data = ['key' => 'value'];
        $checkStmt = $this->createMock(\PDOStatement::class);
        $insertStmt = $this->createMock(\PDOStatement::class);
        
        $this->pdo->expects($this->exactly(2))
            ->method('prepare')
            ->withConsecutive(
                ["SELECT id FROM {$this->tableName} WHERE message_id = :message_id"],
                ["INSERT INTO {$this->tableName} (message_id, topic, data, status, created_at, updated_at) VALUES (:message_id, :topic, :data, 'processing', :created_at, :updated_at)"]
            )
            ->willReturnOnConsecutiveCalls($checkStmt, $insertStmt);
        
        $checkStmt->expects($this->once())
            ->method('execute')
            ->with(['message_id' => $messageId]);
        
        $checkStmt->expects($this->once())
            ->method('fetch')
            ->with(\PDO::FETCH_ASSOC)
            ->willReturn(false);
        
        $insertStmt->expects($this->once())
            ->method('execute')
            ->with($this->callback(function ($params) use ($messageId, $topic) {
                return $params['message_id'] === $messageId &&
                       $params['topic'] === $topic &&
                       $params['data'] === json_encode(['key' => 'value']) &&
                       isset($params['created_at']) &&
                       isset($params['updated_at']);
            }))
            ->willReturn(true);
        
        $result = $this->idempotencyHelper->markAsProcessing($messageId, $topic, $data);
        
        $this->assertTrue($result, '标记新记录为处理中应返回true');
    }

    public function testMarkAsProcessingForExistingRecord()
    {
        $messageId = 'test-message-id';
        $checkStmt = $this->createMock(\PDOStatement::class);
        $updateStmt = $this->createMock(\PDOStatement::class);
        
        $this->pdo->expects($this->exactly(2))
            ->method('prepare')
            ->withConsecutive(
                ["SELECT id FROM {$this->tableName} WHERE message_id = :message_id"],
                ["UPDATE {$this->tableName} SET status = 'processing', updated_at = :updated_at WHERE message_id = :message_id"]
            )
            ->willReturnOnConsecutiveCalls($checkStmt, $updateStmt);
        
        $checkStmt->expects($this->once())
            ->method('execute')
            ->with(['message_id' => $messageId]);
        
        $checkStmt->expects($this->once())
            ->method('fetch')
            ->with(\PDO::FETCH_ASSOC)
            ->willReturn(['id' => 1]);
        
        $updateStmt->expects($this->once())
            ->method('execute')
            ->with($this->callback(function ($params) use ($messageId) {
                return $params['message_id'] === $messageId &&
                       isset($params['updated_at']);
            }))
            ->willReturn(true);
        
        $result = $this->idempotencyHelper->markAsProcessing($messageId);
        
        $this->assertTrue($result, '标记已存在记录为处理中应返回true');
    }

    public function testMarkAsProcessed()
    {
        $messageId = 'test-message-id';
        $stmt = $this->createMock(\PDOStatement::class);
        
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with("UPDATE {$this->tableName} SET status = 'processed', updated_at = :updated_at WHERE message_id = :message_id")
            ->willReturn($stmt);
        
        $stmt->expects($this->once())
            ->method('execute')
            ->with($this->callback(function ($params) use ($messageId) {
                return $params['message_id'] === $messageId &&
                       isset($params['updated_at']);
            }))
            ->willReturn(true);
        
        $result = $this->idempotencyHelper->markAsProcessed($messageId);
        
        $this->assertTrue($result, '标记为已处理应返回true');
    }

    public function testMarkAsFailed()
    {
        $messageId = 'test-message-id';
        $errorMessage = 'Test error message';
        $stmt = $this->createMock(\PDOStatement::class);
        
        $this->pdo->expects($this->once())
            ->method('prepare')
            ->with("UPDATE {$this->tableName} SET status = 'failed', error = :error, updated_at = :updated_at WHERE message_id = :message_id")
            ->willReturn($stmt);
        
        $stmt->expects($this->once())
            ->method('execute')
            ->with($this->callback(function ($params) use ($messageId, $errorMessage) {
                return $params['message_id'] === $messageId &&
                       $params['error'] === $errorMessage &&
                       isset($params['updated_at']);
            }))
            ->willReturn(true);
        
        $result = $this->idempotencyHelper->markAsFailed($messageId, $errorMessage);
        
        $this->assertTrue($result, '标记为失败应返回true');
    }

    public function testDeleteRecord()
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
        
        $result = $this->idempotencyHelper->deleteRecord($messageId);
        
        $this->assertTrue($result, '删除记录应返回true');
    }
}