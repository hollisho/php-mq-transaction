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
     * 构造函数
     * 
     * @param \PDO $pdo 数据库连接
     * @param string $tableName 消息表名
     */
    public function __construct(\PDO $pdo, string $tableName = 'mq_messages')
    {
        $this->pdo = $pdo;
        $this->tableName = $tableName;
    }
    
    /**
     * 开始事务
     * 
     * @return bool 操作结果
     */
    public function beginTransaction(): bool
    {
        return $this->pdo->beginTransaction();
    }
    
    /**
     * 提交事务
     * 
     * @return bool 操作结果
     */
    public function commit(): bool
    {
        return $this->pdo->commit();
    }
    
    /**
     * 回滚事务
     * 
     * @return bool 操作结果
     */
    public function rollback(): bool
    {
        if ($this->pdo->inTransaction()) {
            return $this->pdo->rollBack();
        }
        
        return true;
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
        
        $stmt->bindValue(':limit', $limit, \PDO::PARAM_INT);
        $stmt->execute();
        
        $messages = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        
        // 解析JSON数据
        foreach ($messages as &$message) {
            if (isset($message['data']) && is_string($message['data'])) {
                $message['data'] = json_decode($message['data'], true);
            }
            if (isset($message['options']) && is_string($message['options'])) {
                $message['options'] = json_decode($message['options'], true);
            }
        }
        
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
        
        $stmt->bindValue(':limit', $limit, \PDO::PARAM_INT);
        $stmt->execute();
        
        $messages = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        
        // 解析JSON数据
        foreach ($messages as &$message) {
            if (isset($message['data']) && is_string($message['data'])) {
                $message['data'] = json_decode($message['data'], true);
            }
            if (isset($message['options']) && is_string($message['options'])) {
                $message['options'] = json_decode($message['options'], true);
            }
        }
        
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
        $stmt = $this->pdo->prepare("UPDATE {$this->tableName} 
            SET status = 'sent', updated_at = :updated_at 
            WHERE message_id = :message_id");
        
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
        $stmt = $this->pdo->prepare("UPDATE {$this->tableName} 
            SET status = 'failed', error = :error, updated_at = :updated_at 
            WHERE message_id = :message_id");
        
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
        $stmt = $this->pdo->prepare("UPDATE {$this->tableName} 
            SET status = 'compensated', updated_at = :updated_at 
            WHERE message_id = :message_id");
        
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
}