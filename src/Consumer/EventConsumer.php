<?php

namespace hollisho\MQTransaction\Consumer;

use hollisho\MQTransaction\Database\IdempotencyHelper;
use hollisho\MQTransaction\MQ\MQConnectorInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * 事件消费者
 * 
 * 负责消息消费与幂等控制
 */
class EventConsumer
{
    /**
     * @var MQConnectorInterface 消息队列连接器
     */
    private $mqConnector;
    
    /**
     * @var IdempotencyHelper 幂等助手
     */
    private $idempotencyHelper;
    
    /**
     * @var LoggerInterface 日志记录器
     */
    private $logger;
    
    /**
     * @var array 消息处理器映射
     */
    private $handlers = [];
    
    /**
     * 构造函数
     * 
     * @param MQConnectorInterface $mqConnector 消息队列连接器
     * @param IdempotencyHelper $idempotencyHelper 幂等助手
     * @param LoggerInterface|null $logger 日志记录器
     */
    public function __construct(
        MQConnectorInterface $mqConnector, 
        IdempotencyHelper $idempotencyHelper,
        ?LoggerInterface $logger = null
    ) {
        $this->mqConnector = $mqConnector;
        $this->idempotencyHelper = $idempotencyHelper;
        $this->logger = $logger ?? new NullLogger();
    }
    
    /**
     * 注册消息处理器
     * 
     * @param string $topic 消息主题
     * @param callable $handler 处理器函数
     * @return self
     */
    public function registerHandler(string $topic, callable $handler): self
    {
        $this->handlers[$topic] = $handler;
        return $this;
    }
    
    /**
     * 处理消息
     * 
     * @param array $message 消息数据
     * @return bool 处理结果
     */
    public function processMessage(array $message): bool
    {
        $messageId = $message['message_id'] ?? null;
        $topic = $message['topic'] ?? null;
        
        if (!$messageId || !$topic) {
            $this->logger->error('Invalid message format', ['message' => $message]);
            return false;
        }
        
        // 检查是否已处理过该消息（幂等控制）
        if ($this->idempotencyHelper->isProcessed($messageId)) {
            $this->logger->info('Message already processed, skipping', ['message_id' => $messageId]);
            return true; // 已处理过，视为成功
        }
        
        // 检查是否有对应的处理器
        if (!isset($this->handlers[$topic])) {
            $this->logger->warning('No handler registered for topic', ['topic' => $topic]);
            return false;
        }
        
        try {
            // 标记消息为处理中
            $this->idempotencyHelper->markAsProcessing($messageId);
            
            // 调用处理器
            $handler = $this->handlers[$topic];
            $result = $handler($message);
            
            if ($result) {
                // 标记消息为已处理
                $this->idempotencyHelper->markAsProcessed($messageId);
                $this->logger->info('Message processed successfully', ['message_id' => $messageId]);
                return true;
            } else {
                // 处理失败
                $this->idempotencyHelper->markAsFailed($messageId, 'Handler returned false');
                $this->logger->error('Message processing failed', ['message_id' => $messageId]);
                return false;
            }
        } catch (\Exception $e) {
            // 处理异常
            $this->idempotencyHelper->markAsFailed($messageId, $e->getMessage());
            $this->logger->error('Exception during message processing', [
                'message_id' => $messageId,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            return false;
        }
    }
    
    /**
     * 启动消费
     * 
     * @param array $topics 要消费的主题列表
     * @return void
     */
    public function start(array $topics = []): void
    {
        // 如果未指定主题，则使用所有已注册处理器的主题
        if (empty($topics)) {
            $topics = array_keys($this->handlers);
        }
        
        if (empty($topics)) {
            $this->logger->warning('No topics to consume');
            return;
        }
        
        $this->logger->info('Starting consumer', ['topics' => $topics]);
        
        // 启动消费
        $this->mqConnector->consume($topics, function ($message) {
            return $this->processMessage($message);
        });
    }
}