<?php

namespace hollisho\MQTransaction\Producer;

use hollisho\MQTransaction\Database\TransactionHelper;
use hollisho\MQTransaction\MQ\MQConnectorInterface;
use Ramsey\Uuid\Uuid;

/**
 * 事务性消息生产者
 * 
 * 负责将消息发送与本地事务绑定，确保原子性
 */
class TransactionProducer
{
    /**
     * @var MQConnectorInterface 消息队列连接器
     */
    private $mqConnector;
    
    /**
     * @var TransactionHelper 事务助手
     */
    private $transactionHelper;
    
    /**
     * @var array 当前事务中预备发送的消息
     */
    private $preparedMessages = [];
    
    /**
     * @var bool 是否处于事务中
     */
    private $inTransaction = false;
    
    /**
     * 构造函数
     * 
     * @param MQConnectorInterface $mqConnector 消息队列连接器
     * @param TransactionHelper $transactionHelper 事务助手
     */
    public function __construct(MQConnectorInterface $mqConnector, TransactionHelper $transactionHelper)
    {
        $this->mqConnector = $mqConnector;
        $this->transactionHelper = $transactionHelper;
    }
    
    /**
     * 开始事务
     * 
     * @return void
     */
    public function beginTransaction()
    {
        if ($this->inTransaction) {
            throw new \RuntimeException('Transaction already started');
        }
        
        $this->transactionHelper->beginTransaction();
        $this->preparedMessages = [];
        $this->inTransaction = true;
    }
    
    /**
     * 准备发送消息（将在事务提交后异步发送）
     * 
     * @param string $topic 消息主题
     * @param array $data 消息数据
     * @param array $options 额外选项
     * @return string 消息ID
     */
    public function prepareMessage(string $topic, array $data, array $options = []): string
    {
        if (!$this->inTransaction) {
            throw new \RuntimeException('No active transaction');
        }
        
        $messageId = Uuid::uuid4()->toString();
        $message = [
            'message_id' => $messageId,
            'topic' => $topic,
            'data' => $data,
            'options' => $options,
            'status' => 'pending',
            'created_at' => date('Y-m-d H:i:s'),
            'retry_count' => 0
        ];
        
        $this->preparedMessages[] = $message;
        return $messageId;
    }
    
    /**
     * 提交事务
     * 
     * @return void
     */
    public function commit()
    {
        if (!$this->inTransaction) {
            throw new \RuntimeException('No active transaction');
        }
        
        try {
            // 保存预备消息到消息表
            foreach ($this->preparedMessages as $message) {
                $result = $this->transactionHelper->saveMessage($message);
                if (!$result) {
                    throw new \RuntimeException('Failed to save message');
                }
            }
            
            // 提交数据库事务
            $this->transactionHelper->commit();
            
            // 重置状态
            $this->preparedMessages = [];
            $this->inTransaction = false;
        } catch (\Exception $e) {
            $this->rollback();
            throw $e;
        }
    }
    
    /**
     * 回滚事务
     * 
     * @return void
     */
    public function rollback()
    {
        if (!$this->inTransaction) {
            throw new \RuntimeException('No active transaction');
        }
        
        $this->transactionHelper->rollback();
        $this->preparedMessages = [];
        $this->inTransaction = false;
    }
}