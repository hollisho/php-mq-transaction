<?php

namespace hollisho\MQTransaction\Producer;

use hollisho\MQTransaction\Database\TransactionHelper;
use hollisho\MQTransaction\MQ\MQConnectorInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * 消息分发器
 * 
 * 负责异步发送预备消息到消息队列
 */
class MessageDispatcher
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
     * @var LoggerInterface 日志记录器
     */
    private $logger;
    
    /**
     * @var int 每次处理的消息数量
     */
    private $batchSize = 100;
    
    /**
     * @var int 最大重试次数
     */
    private $maxRetryCount = 5;
    
    /**
     * 构造函数
     * 
     * @param MQConnectorInterface $mqConnector 消息队列连接器
     * @param TransactionHelper $transactionHelper 事务助手
     * @param LoggerInterface|null $logger 日志记录器
     */
    public function __construct(
        MQConnectorInterface $mqConnector, 
        TransactionHelper $transactionHelper,
        LoggerInterface $logger = null
    ) {
        $this->mqConnector = $mqConnector;
        $this->transactionHelper = $transactionHelper;
        $this->logger = $logger ?? new NullLogger();
    }
    
    /**
     * 设置每次处理的消息数量
     * 
     * @param int $batchSize 批处理大小
     * @return self
     */
    public function setBatchSize(int $batchSize): self
    {
        $this->batchSize = $batchSize;
        return $this;
    }
    
    /**
     * 设置最大重试次数
     * 
     * @param int $maxRetryCount 最大重试次数
     * @return self
     */
    public function setMaxRetryCount(int $maxRetryCount): self
    {
        $this->maxRetryCount = $maxRetryCount;
        return $this;
    }
    
    /**
     * 执行消息分发
     * 
     * @return int 成功发送的消息数量
     */
    public function dispatch(): int
    {
        $pendingMessages = $this->transactionHelper->getPendingMessages($this->batchSize);
        $sentCount = 0;
        
        foreach ($pendingMessages as $message) {
            try {
                // 确保数据是数组格式
                $data = is_string($message['data']) ? json_decode($message['data'], true) : $message['data'];
                $options = [];
                
                // 处理options字段
                if (!empty($message['options'])) {
                    $options = is_string($message['options']) ? json_decode($message['options'], true) : $message['options'];
                }
                
                // 发送消息到MQ
                $result = $this->mqConnector->send(
                    $message['topic'],
                    $data,
                    $message['message_id'],
                    $options
                );
                
                // 仅当发送成功时才更新状态并计数
                if ($result) {
                    // 更新消息状态为已发送
                    $this->transactionHelper->markMessageAsSent($message['message_id']);
                    $sentCount++;
                    
                    $this->logger->info('Message sent successfully', [
                        'message_id' => $message['message_id'],
                        'topic' => $message['topic']
                    ]);
                } else {
                    // 发送失败的处理逻辑
                    $this->logger->warning('Failed to send message', [
                        'message_id' => $message['message_id'],
                        'topic' => $message['topic'],
                        'retry_count' => $message['retry_count']
                    ]);
                    
                    // 更新重试次数
                    $retryCount = $message['retry_count'] + 1;
                    
                    if ($retryCount >= $this->maxRetryCount) {
                        // 超过最大重试次数，标记为失败
                        $this->transactionHelper->markMessageAsFailed(
                            $message['message_id'],
                            'Max retry count exceeded'
                        );
                        
                        $this->logger->error('Max retry count exceeded for message', [
                            'message_id' => $message['message_id'],
                            'topic' => $message['topic'],
                            'retry_count' => $retryCount
                        ]);
                    } else {
                        // 更新重试次数
                        $this->transactionHelper->incrementRetryCount($message['message_id']);
                    }
                }
            } catch (\Exception $e) {
                $this->logger->error('Exception during message dispatch', [
                    'message_id' => $message['message_id'],
                    'topic' => $message['topic'],
                    'error' => $e->getMessage(),
                    'retry_count' => $message['retry_count']
                ]);
                
                // 更新重试次数
                $this->transactionHelper->incrementRetryCount($message['message_id']);
            }
        }
        
        return $sentCount;
    }
    
    /**
     * 启动消息分发循环
     * 
     * @param int $interval 轮询间隔（秒）
     * @param int|null $maxIterations 最大迭代次数（用于测试，生产环境应为null）
     * @return void
     */
    public function run(int $interval = 5, int $maxIterations = null)
    {
        $iterations = 0;
        
        while (true) {
            $sentCount = $this->dispatch();
            $this->logger->info("Dispatched {$sentCount} messages");
            
            // 检查是否达到最大迭代次数
            if ($maxIterations !== null) {
                $iterations++;
                if ($iterations >= $maxIterations) {
                    break;
                }
            }
            
            // 等待下一次轮询
            sleep($interval);
        }
    }
}