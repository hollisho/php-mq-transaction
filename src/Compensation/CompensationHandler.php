<?php

namespace hollisho\MQTransaction\Compensation;

use Psr\Container\ContainerExceptionInterface;
use Psr\Container\ContainerInterface;
use Psr\Container\NotFoundExceptionInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * 补偿处理器
 * 
 * 负责处理分布式事务中的异常情况，执行补偿逻辑
 */
class CompensationHandler
{
    /**
     * @var ContainerInterface|null 服务容器
     */
    private $container;
    
    /**
     * @var LoggerInterface 日志记录器
     */
    private $logger;
    
    /**
     * @var array 生产者补偿处理器映射
     */
    private $producerHandlers = [];
    
    /**
     * @var array 消费者补偿处理器映射
     */
    private $consumerHandlers = [];
    
    /**
     * 构造函数
     * 
     * @param ContainerInterface|null $container 服务容器
     * @param LoggerInterface|null $logger 日志记录器
     */
    public function __construct(ContainerInterface $container = null, LoggerInterface $logger = null)
    {
        $this->container = $container;
        $this->logger = $logger ?? new NullLogger();
    }
    
    /**
     * 注册生产者补偿处理器
     * 
     * @param string $topic 消息主题
     * @param callable|string $handler 处理器函数或服务ID
     * @return self
     */
    public function registerProducerHandler(string $topic, $handler): self
    {
        $this->producerHandlers[$topic] = $handler;
        return $this;
    }
    
    /**
     * 注册消费者补偿处理器
     * 
     * @param string $topic 消息主题
     * @param callable|string $handler 处理器函数或服务ID
     * @return self
     */
    public function registerConsumerHandler(string $topic, $handler): self
    {
        $this->consumerHandlers[$topic] = $handler;
        return $this;
    }

    /**
     * 处理生产者失败
     *
     * @param array $message 失败的消息
     * @return bool 处理结果
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function handleProducerFailure(array $message): bool
    {
        $topic = $message['topic'] ?? null;
        
        if (!$topic || !isset($this->producerHandlers[$topic])) {
            $this->logger->warning('No producer compensation handler for topic', ['topic' => $topic]);
            return false;
        }
        
        try {
            $handler = $this->resolveHandler($this->producerHandlers[$topic]);
            return $handler($message);
        } catch (\Exception $e) {
            $this->logger->error('Producer compensation failed', [
                'message_id' => $message['message_id'],
                'error' => $e->getMessage()
            ]);
            return false;
        }
    }

    /**
     * 处理消费者失败
     *
     * @param array $consumption 失败的消费记录
     * @return bool 处理结果
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    public function handleConsumerFailure(array $consumption): bool
    {
        $topic = $consumption['topic'] ?? null;
        
        if (!$topic || !isset($this->consumerHandlers[$topic])) {
            $this->logger->warning('No consumer compensation handler for topic', ['topic' => $topic]);
            return false;
        }
        
        try {
            $handler = $this->resolveHandler($this->consumerHandlers[$topic]);
            return $handler($consumption);
        } catch (\Exception $e) {
            $this->logger->error('Consumer compensation failed', [
                'message_id' => $consumption['message_id'],
                'error' => $e->getMessage()
            ]);
            return false;
        }
    }

    /**
     * 解析处理器
     * @param callable|string $handler 处理器函数或服务ID
     * @return callable 可调用的处理器
     * @throws ContainerExceptionInterface
     * @throws NotFoundExceptionInterface
     */
    private function resolveHandler($handler): callable
    {
        if (is_callable($handler)) {
            return $handler;
        }
        
        if (is_string($handler) && $this->container !== null) {
            $service = $this->container->get($handler);
            
            if (is_callable($service)) {
                return $service;
            }
            
            throw new \RuntimeException("Service '{$handler}' is not callable");
        }
        
        throw new \RuntimeException('Invalid handler type');
    }
}