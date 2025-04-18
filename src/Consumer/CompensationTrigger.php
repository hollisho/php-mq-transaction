<?php

namespace hollisho\MQTransaction\Consumer;

use hollisho\MQTransaction\Compensation\CompensationHandler;
use hollisho\MQTransaction\Database\IdempotencyHelper;
use hollisho\MQTransaction\Database\TransactionHelper;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * 补偿触发器
 * 
 * 负责检测失败的消息并触发补偿操作
 */
class CompensationTrigger
{
    /**
     * @var TransactionHelper 事务助手
     */
    private $transactionHelper;
    
    /**
     * @var IdempotencyHelper 幂等助手
     */
    private $idempotencyHelper;
    
    /**
     * @var CompensationHandler 补偿处理器
     */
    private $compensationHandler;
    
    /**
     * @var LoggerInterface 日志记录器
     */
    private $logger;
    
    /**
     * @var int 每次处理的消息数量
     */
    private $batchSize = 50;
    
    /**
     * 构造函数
     * 
     * @param TransactionHelper $transactionHelper 事务助手
     * @param IdempotencyHelper $idempotencyHelper 幂等助手
     * @param CompensationHandler $compensationHandler 补偿处理器
     * @param LoggerInterface|null $logger 日志记录器
     */
    public function __construct(
        TransactionHelper $transactionHelper,
        IdempotencyHelper $idempotencyHelper,
        CompensationHandler $compensationHandler,
        ?LoggerInterface $logger = null
    ) {
        $this->transactionHelper = $transactionHelper;
        $this->idempotencyHelper = $idempotencyHelper;
        $this->compensationHandler = $compensationHandler;
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
     * 检查并触发生产者端补偿
     * 
     * @return int 处理的消息数量
     */
    public function checkProducerCompensation(): int
    {
        $failedMessages = $this->transactionHelper->getFailedMessages($this->batchSize);
        $processedCount = 0;
        
        foreach ($failedMessages as $message) {
            try {
                $this->logger->info('Triggering producer compensation', [
                    'message_id' => $message['message_id'],
                    'topic' => $message['topic']
                ]);
                
                // 调用补偿处理器
                $result = $this->compensationHandler->handleProducerFailure($message);
                
                if ($result) {
                    // 标记为已补偿
                    $this->transactionHelper->markMessageAsCompensated($message['message_id']);
                    $processedCount++;
                } else {
                    // 补偿失败，记录日志
                    $this->logger->error('Producer compensation failed', [
                        'message_id' => $message['message_id']
                    ]);
                }
            } catch (\Exception $e) {
                $this->logger->error('Exception during producer compensation', [
                    'message_id' => $message['message_id'],
                    'error' => $e->getMessage()
                ]);
            }
        }
        
        return $processedCount;
    }
    
    /**
     * 检查并触发消费者端补偿
     * 
     * @return int 处理的消息数量
     */
    public function checkConsumerCompensation(): int
    {
        $failedConsumptions = $this->idempotencyHelper->getFailedConsumptions($this->batchSize);
        $processedCount = 0;
        
        foreach ($failedConsumptions as $consumption) {
            try {
                $this->logger->info('Triggering consumer compensation', [
                    'message_id' => $consumption['message_id']
                ]);
                
                // 调用补偿处理器
                $result = $this->compensationHandler->handleConsumerFailure($consumption);
                
                if ($result) {
                    // 标记为已补偿
                    $this->idempotencyHelper->markAsCompensated($consumption['message_id']);
                    $processedCount++;
                } else {
                    // 补偿失败，记录日志
                    $this->logger->error('Consumer compensation failed', [
                        'message_id' => $consumption['message_id']
                    ]);
                }
            } catch (\Exception $e) {
                $this->logger->error('Exception during consumer compensation', [
                    'message_id' => $consumption['message_id'],
                    'error' => $e->getMessage()
                ]);
            }
        }
        
        return $processedCount;
    }
    
    /**
     * 运行补偿检查
     * 
     * @param int $interval 检查间隔（秒）
     * @param int|null $maxIterations 最大迭代次数（用于测试，生产环境应为null）
     * @return void
     */
    public function run(int $interval = 60, ?int $maxIterations = null): void
    {
        $iterations = 0;
        
        while (true) {
            $producerCount = $this->checkProducerCompensation();
            $consumerCount = $this->checkConsumerCompensation();
            
            $this->logger->info("Compensation check completed", [
                'producer_compensations' => $producerCount,
                'consumer_compensations' => $consumerCount
            ]);
            
            // 检查是否达到最大迭代次数
            if ($maxIterations !== null) {
                $iterations++;
                if ($iterations >= $maxIterations) {
                    break;
                }
            }
            
            // 等待下一次检查
            sleep($interval);
        }
    }
}