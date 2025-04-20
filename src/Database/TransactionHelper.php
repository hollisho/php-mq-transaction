<?php

namespace hollisho\MQTransaction\Database;

/**
 * 事务助手
 * 
 * 负责管理本地事务和消息表
 */
class TransactionHelper
{
    /**
     * @var \PDO 数据库连接
     */
    private $pdo;
    
    /**
     * @var string 消息表名
     */
    private $tableName;
    
    /**
     * @var int 事务嵌套计数器
     */
    private $transactionCounter = 0;
    
    /**
     * @var bool 是否启用调试信息
     */
    private $debugMode = false;
    
    /**
     * 构造函数
     * 
     * @param \PDO $pdo 数据库连接
     * @param string $tableName 消息表名
     * @param bool $debug 是否启用调试
     */
    public function __construct(\PDO $pdo, string $tableName = 'mq_messages', bool $debug = false)
    {
        $this->pdo = $pdo;
        $this->tableName = $tableName;
        $this->debugMode = $debug;
    }
    
    /**
     * 开始事务，支持嵌套
     * 
     * @return bool 操作结果
     */
    public function beginTransaction(): bool
    {
        // 如果是第一层事务，真正开始事务
        if ($this->transactionCounter == 0) {
            $this->transactionCounter++;
            return $this->pdo->beginTransaction();
        }
        
        // 如果已经在事务中，只增加计数器
        $this->transactionCounter++;
        return true;
    }
    
    /**
     * 提交事务，支持嵌套
     * 
     * @return bool 操作结果
     */
    public function commit(): bool
    {
        // 确保在事务中
        if ($this->transactionCounter == 0) {
            // 如果没有活跃事务，记录问题但不抛出异常
            if ($this->debugMode) {
                error_log("Warning: Trying to commit when no active transaction exists");
            }
            return false;
        }
        
        // 减少计数器
        $this->transactionCounter--;
        
        // 如果是最外层事务，真正提交
        if ($this->transactionCounter == 0) {
            // 检查PDO是否认为有事务在进行
            if ($this->pdo->inTransaction()) {
                return $this->pdo->commit();
            } else {
                // PDO认为没有事务，可能是由于内部回滚或其他原因
                if ($this->debugMode) {
                    error_log("Warning: PDO reports no active transaction, but counter was positive");
                }
                // 重置计数器以保持一致性
                $this->transactionCounter = 0;
                return false;
            }
        }
        
        // 内层事务不做真正提交
        return true;
    }
    
    /**
     * 回滚事务，支持嵌套
     * 
     * @return bool 操作结果
     */
    public function rollback(): bool
    {
        // 确保在事务中
        if ($this->transactionCounter == 0) {
            if ($this->debugMode) {
                error_log("Warning: Trying to rollback when no active transaction exists");
            }
            return false; // 没有事务，不需要回滚
        }
        
        // 重置计数器
        $previousCounter = $this->transactionCounter;
        $this->transactionCounter = 0;
        
        // 执行真正的回滚
        if ($this->pdo->inTransaction()) {
            try {
                return $this->pdo->rollBack();
            } catch (\PDOException $e) {
                // 某些PDO驱动在没有活跃事务时调用rollBack会抛出异常
                if ($this->debugMode) {
                    error_log("Warning: Exception during rollback: " . $e->getMessage());
                }
                return false;
            }
        } else {
            // PDO认为没有事务进行中，但我们的计数器是正数
            if ($this->debugMode && $previousCounter > 0) {
                error_log("Warning: PDO reports no active transaction, but counter was {$previousCounter}");
            }
            return false;
        }
    }
    
    /**
     * 保存消息到消息表
     * 
     * @param array $message 消息数据
     * @return bool 操作结果
     */
    public function saveMessage(array $message): bool
    {
        $stmt = $this->pdo->prepare("INSERT INTO {$this->tableName} 
            (message_id, topic, data, options, status, created_at, updated_at, retry_count) 
            VALUES (:message_id, :topic, :data, :options, :status, :created_at, :updated_at, :retry_count)");
        
        return $stmt->execute([
            'message_id' => $message['message_id'],
            'topic' => $message['topic'],
            'data' => json_encode($message['data']),
            'options' => json_encode($message['options'] ?? []),
            'status' => $message['status'],
            'created_at' => $message['created_at'],
            'updated_at' => $message['created_at'],
            'retry_count' => $message['retry_count'] ?? 0
        ]);
    }
    
    /**
     * 获取待发送的消息
     * 
     * @param int $limit 限制数量
     * @return array 待发送的消息列表
     */
    public function getPendingMessages(int $limit = 100): array
    {
        $stmt = $this->pdo->prepare("SELECT * FROM {$this->tableName} 
            WHERE status = 'pending' 
            ORDER BY created_at ASC 
            LIMIT :limit");
        
        $stmt->execute(['limit' => $limit]);
        
        $messages = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        

        
        return $messages;
    }
    
    /**
     * 获取失败的消息
     * 
     * @param int $limit 限制数量
     * @return array 失败的消息列表
     */
    public function getFailedMessages(int $limit = 100): array
    {
        $stmt = $this->pdo->prepare("SELECT * FROM {$this->tableName} 
            WHERE status = 'failed' 
            ORDER BY updated_at ASC 
            LIMIT :limit");
        
        $stmt->execute(['limit' => $limit]);
        
        $messages = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        

        
        return $messages;
    }
    
    /**
     * 标记消息为已发送
     * 
     * @param string $messageId 消息ID
     * @return bool 操作结果
     */
    public function markMessageAsSent(string $messageId): bool
    {
        $stmt = $this->pdo->prepare("update {$this->tableName} set status = 'sent', updated_at = :updated_at 
            where message_id = :message_id");
        
        return $stmt->execute([
            'message_id' => $messageId,
            'updated_at' => date('Y-m-d H:i:s')
        ]);
    }
    
    /**
     * 标记消息为失败
     * 
     * @param string $messageId 消息ID
     * @param string $error 错误信息
     * @return bool 操作结果
     */
    public function markMessageAsFailed(string $messageId, string $error): bool
    {
        $stmt = $this->pdo->prepare("update {$this->tableName} 
            set status = 'failed', error = :error, updated_at = :updated_at 
            where message_id = :message_id");
        
        return $stmt->execute([
            'message_id' => $messageId,
            'error' => $error,
            'updated_at' => date('Y-m-d H:i:s')
        ]);
    }
    
    /**
     * 标记消息为已补偿
     * 
     * @param string $messageId 消息ID
     * @return bool 操作结果
     */
    public function markMessageAsCompensated(string $messageId): bool
    {
        $stmt = $this->pdo->prepare("update {$this->tableName} 
            set status = 'compensated', updated_at = :updated_at 
            where message_id = :message_id");
        
        return $stmt->execute([
            'message_id' => $messageId,
            'updated_at' => date('Y-m-d H:i:s')
        ]);
    }
    
    /**
     * 增加消息重试次数
     * 
     * @param string $messageId 消息ID
     * @param int $retryCount 新的重试次数
     * @return bool 操作结果
     */
    public function incrementMessageRetryCount(string $messageId, int $retryCount): bool
    {
        $stmt = $this->pdo->prepare("UPDATE {$this->tableName} 
            SET retry_count = :retry_count, updated_at = :updated_at 
            WHERE message_id = :message_id");
        
        return $stmt->execute([
            'message_id' => $messageId,
            'retry_count' => $retryCount,
            'updated_at' => date('Y-m-d H:i:s')
        ]);
    }
    
    /**
     * 增加消息重试次数（递增1）
     * 
     * @param string $messageId 消息ID
     * @return bool 操作结果
     */
    public function incrementRetryCount(string $messageId): bool
    {
        $stmt = $this->pdo->prepare("update {$this->tableName} 
            set retry_count = retry_count + 1, updated_at = :updated_at 
            where message_id = :message_id");
        
        return $stmt->execute([
            'message_id' => $messageId,
            'updated_at' => date('Y-m-d H:i:s')
        ]);
    }
    
    /**
     * 创建消息表
     * 
     * @return bool 操作结果
     */
    public function createTable(): bool
    {
        $sql = "CREATE TABLE IF NOT EXISTS {$this->tableName} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            message_id VARCHAR(64) NOT NULL,
            topic VARCHAR(255) NOT NULL,
            data TEXT NOT NULL,
            options TEXT,
            status ENUM('pending', 'sent', 'failed', 'compensated') NOT NULL,
            error TEXT,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            retry_count INT NOT NULL DEFAULT 0,
            UNIQUE KEY (message_id)
        )";
        
        return $this->pdo->exec($sql) !== false;
    }
    
    /**
     * 删除消息
     * 
     * @param string $messageId 消息ID
     * @return bool 操作结果
     */
    public function deleteMessage(string $messageId): bool
    {
        $stmt = $this->pdo->prepare("DELETE FROM {$this->tableName} WHERE message_id = :message_id");
        
        return $stmt->execute([
            'message_id' => $messageId
        ]);
    }
}