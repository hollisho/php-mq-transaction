<?php

namespace hollisho\MQTransaction\Database;

/**
 * 幂等性助手
 * 
 * 负责管理消息消费的幂等性，防止重复消费
 */
class IdempotencyHelper
{
    /**
     * @var \PDO 数据库连接
     */
    private $pdo;
    
    /**
     * @var string 消费记录表名
     */
    private $tableName;
    
    /**
     * 构造函数
     * 
     * @param \PDO $pdo 数据库连接
     * @param string $tableName 消费记录表名
     */
    public function __construct(\PDO $pdo, string $tableName = 'mq_consumption_records')
    {
        $this->pdo = $pdo;
        $this->tableName = $tableName;
    }
    
    /**
     * 检查消息是否已处理
     * 
     * @param string $messageId 消息ID
     * @return bool 是否已处理
     */
    public function isProcessed(string $messageId): bool
    {
        $stmt = $this->pdo->prepare("SELECT status FROM {$this->tableName} WHERE message_id = :message_id");
        $stmt->execute(['message_id' => $messageId]);
        $record = $stmt->fetch(\PDO::FETCH_ASSOC);
        
        return $record && $record['status'] === 'processed';
    }
    
    /**
     * 标记消息为处理中
     * 
     * @param string $messageId 消息ID
     * @param string $topic 消息主题
     * @param array $data 消息数据
     * @return bool 操作结果
     */
    public function markAsProcessing(string $messageId, string $topic = null, array $data = null): bool
    {
        try {
            // 检查记录是否存在
            $stmt = $this->pdo->prepare("SELECT id FROM {$this->tableName} WHERE message_id = :message_id");
            $stmt->execute(['message_id' => $messageId]);
            $exists = $stmt->fetch(\PDO::FETCH_ASSOC);
            
            if ($exists) {
                // 更新状态
                $stmt = $this->pdo->prepare("UPDATE {$this->tableName} SET status = 'processing', updated_at = :updated_at WHERE message_id = :message_id");
                return $stmt->execute([
                    'message_id' => $messageId,
                    'updated_at' => date('Y-m-d H:i:s')
                ]);
            } else {
                // 创建新记录
                $stmt = $this->pdo->prepare("INSERT INTO {$this->tableName} (message_id, topic, data, status, created_at, updated_at) VALUES (:message_id, :topic, :data, 'processing', :created_at, :updated_at)");
                return $stmt->execute([
                    'message_id' => $messageId,
                    'topic' => $topic,
                    'data' => $data ? json_encode($data) : null,
                    'created_at' => date('Y-m-d H:i:s'),
                    'updated_at' => date('Y-m-d H:i:s')
                ]);
            }
        } catch (\Exception $e) {
            // 记录异常并返回失败
            error_log("Failed to mark message as processing: {$e->getMessage()}");
            return false;
        }
    }
    
    /**
     * 标记消息为已处理
     * 
     * @param string $messageId 消息ID
     * @return bool 操作结果
     */
    public function markAsProcessed(string $messageId): bool
    {
        $stmt = $this->pdo->prepare("UPDATE {$this->tableName} SET status = 'processed', updated_at = :updated_at WHERE message_id = :message_id");
        return $stmt->execute([
            'message_id' => $messageId,
            'updated_at' => date('Y-m-d H:i:s')
        ]);
    }
    
    /**
     * 标记消息为处理失败
     * 
     * @param string $messageId 消息ID
     * @param string $error 错误信息
     * @return bool 操作结果
     */
    public function markAsFailed(string $messageId, string $error): bool
    {
        $stmt = $this->pdo->prepare("UPDATE {$this->tableName} SET status = 'failed', error = :error, updated_at = :updated_at WHERE message_id = :message_id");
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
    public function markAsCompensated(string $messageId): bool
    {
        $stmt = $this->pdo->prepare("UPDATE {$this->tableName} SET status = 'compensated', updated_at = :updated_at WHERE message_id = :message_id");
        return $stmt->execute([
            'message_id' => $messageId,
            'updated_at' => date('Y-m-d H:i:s')
        ]);
    }
    
    /**
     * 获取失败的消费记录
     * 
     * @param int $limit 限制数量
     * @return array 失败的消费记录
     */
    public function getFailedConsumptions(int $limit = 100): array
    {
        $stmt = $this->pdo->prepare("SELECT * FROM {$this->tableName} WHERE status = 'failed' ORDER BY updated_at ASC LIMIT :limit");
        $stmt->bindValue(':limit', $limit, \PDO::PARAM_INT);
        $stmt->execute();
        
        $records = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        
        // 解析JSON数据
        foreach ($records as &$record) {
            if (isset($record['data']) && is_string($record['data'])) {
                $record['data'] = json_decode($record['data'], true);
            }
        }
        
        return $records;
    }
    
    /**
     * 创建消费记录表
     * 
     * @return bool 操作结果
     */
    public function createTable(): bool
    {
        $sql = "CREATE TABLE IF NOT EXISTS {$this->tableName} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            message_id VARCHAR(64) NOT NULL,
            topic VARCHAR(255),
            data TEXT,
            status ENUM('processing', 'processed', 'failed', 'compensated') NOT NULL,
            error TEXT,
            created_at DATETIME NOT NULL,
            updated_at DATETIME NOT NULL,
            UNIQUE KEY (message_id)
        )";
        
        return $this->pdo->exec($sql) !== false;
    }
    
    /**
     * 删除消息记录
     * 
     * @param string $messageId 消息ID
     * @return bool 操作结果
     */
    public function deleteRecord(string $messageId): bool
    {
        $stmt = $this->pdo->prepare("DELETE FROM {$this->tableName} WHERE message_id = :message_id");
        return $stmt->execute([
            'message_id' => $messageId
        ]);
    }
}