<?php

namespace hollisho\MQTransaction\MQ;

use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Kafka连接器
 * 
 * 实现基于Kafka的消息队列连接
 */
class KafkaConnector implements MQConnectorInterface
{
    /**
     * @var array Kafka连接配置
     */
    private $config;
    
    /**
     * @var \RdKafka\Producer|null Kafka生产者
     */
    private $producer;
    
    /**
     * @var \RdKafka\KafkaConsumer|null Kafka消费者
     */
    private $consumer;
    
    /**
     * @var LoggerInterface 日志记录器
     */
    private $logger;
    
    /**
     * @var array 主题配置
     */
    private $topics = [];
    
    /**
     * 构造函数
     * 
     * @param array $config Kafka连接配置
     * @param LoggerInterface|null $logger 日志记录器
     */
    public function __construct(array $config, ?LoggerInterface $logger = null)
    {
        $this->config = array_merge([
            'brokers' => 'localhost:9092',
            'group.id' => 'php-mq-transaction',
            'auto.offset.reset' => 'earliest',
            'enable.auto.commit' => 'false',
        ], $config);
        
        $this->logger = $logger ?? new NullLogger();
    }
    
    /**
     * 获取生产者
     * 
     * @return \RdKafka\Producer
     */
    private function getProducer(): \RdKafka\Producer
    {
        if (!$this->producer) {
            $conf = new \RdKafka\Conf();
            $conf->set('bootstrap.servers', $this->config['brokers']);
            
            // 设置消息发送回调
            $conf->set('dr_msg_cb', function (\RdKafka\Producer $producer, \RdKafka\Message $message) {
                if ($message->err) {
                    $this->logger->error('Message delivery failed', [
                        'error' => rd_kafka_err2str($message->err),
                        'topic' => $message->topic_name,
                        'partition' => $message->partition,
                        'offset' => $message->offset,
                    ]);
                } else {
                    $this->logger->debug('Message delivered', [
                        'topic' => $message->topic_name,
                        'partition' => $message->partition,
                        'offset' => $message->offset,
                    ]);
                }
            });
            
            $this->producer = new \RdKafka\Producer($conf);
        }
        
        return $this->producer;
    }
    
    /**
     * 获取消费者
     * 
     * @return \RdKafka\KafkaConsumer
     */
    private function getConsumer(): \RdKafka\KafkaConsumer
    {
        if (!$this->consumer) {
            $conf = new \RdKafka\Conf();
            
            // 设置基本配置
            $conf->set('bootstrap.servers', $this->config['brokers']);
            $conf->set('group.id', $this->config['group.id']);
            $conf->set('auto.offset.reset', $this->config['auto.offset.reset']);
            $conf->set('enable.auto.commit', $this->config['enable.auto.commit']);
            
            // 设置重平衡回调
            $conf->set('rebalance_cb', function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
                switch ($err) {
                    case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                        $this->logger->info('Assign partitions', ['count' => count($partitions)]);
                        $kafka->assign($partitions);
                        break;
                    case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                        $this->logger->info('Revoke partitions', ['count' => count($partitions)]);
                        $kafka->assign(NULL);
                        break;
                    default:
                        $this->logger->error('Rebalance failed', ['error' => rd_kafka_err2str($err)]);
                        break;
                }
            });
            
            $this->consumer = new \RdKafka\KafkaConsumer($conf);
        }
        
        return $this->consumer;
    }
    
    /**
     * 发送消息到队列
     * 
     * @param string $topic 消息主题
     * @param array $data 消息数据
     * @param string $messageId 消息ID
     * @param array $options 额外选项
     * @return bool 发送结果
     */
    public function send(string $topic, array $data, string $messageId, array $options = []): bool
    {
        try {
            $producer = $this->getProducer();
            
            // 获取或创建主题
            if (!isset($this->topics[$topic])) {
                $this->topics[$topic] = $producer->newTopic($topic);
            }
            
            // 准备消息数据
            $messageData = $data;
            $messageData['_message_id'] = $messageId;
            $messageData['_timestamp'] = time();
            
            // 添加自定义头信息
            if (isset($options['headers'])) {
                $messageData['_headers'] = $options['headers'];
            }
            
            // 序列化消息
            $payload = json_encode($messageData);
            
            // 发送消息
            $partition = $options['partition'] ?? RD_KAFKA_PARTITION_UA;
            $msgFlags = $options['msg_flags'] ?? 0;
            $this->topics[$topic]->produce($partition, $msgFlags, $payload, $messageId);
            
            // 轮询以触发回调
            $producer->poll(0);
            
            // 确保消息被发送
            $result = $producer->flush(10000);
            
            if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR) {
                $this->logger->error('Failed to flush message', [
                    'message_id' => $messageId,
                    'topic' => $topic,
                    'error' => rd_kafka_err2str($result)
                ]);
                return false;
            }
            
            $this->logger->info('Message sent', [
                'message_id' => $messageId,
                'topic' => $topic
            ]);
            
            return true;
        } catch (\Exception $e) {
            $this->logger->error('Failed to send message', [
                'message_id' => $messageId,
                'topic' => $topic,
                'error' => $e->getMessage()
            ]);
            
            return false;
        }
    }
    
    /**
     * 消费消息
     * 
     * @param array $topics 要消费的主题列表
     * @param callable $callback 消息处理回调函数
     * @return void
     */
    public function consume(array $topics, callable $callback): void
    {
        try {
            $consumer = $this->getConsumer();
            
            // 订阅主题
            $consumer->subscribe($topics);
            
            $this->logger->info('Consumer started', ['topics' => implode(', ', $topics)]);
            
            // 开始消费循环
            while (true) {
                $message = $consumer->consume(10000);
                
                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        // 解析消息
                        $payload = json_decode($message->payload, true);
                        $messageId = $payload['_message_id'] ?? $message->key;
                        
                        // 移除内部字段
                        unset($payload['_message_id']);
                        unset($payload['_timestamp']);
                        unset($payload['_headers']);
                        
                        $messageData = [
                            'message_id' => $messageId,
                            'topic' => $message->topic_name,
                            'data' => $payload,
                            'raw_message' => $message
                        ];
                        
                        try {
                            $result = $callback($messageData);
                            
                            if ($result) {
                                // 确认消息
                                $this->ack($message);
                            } else {
                                // 拒绝消息
                                $this->nack($message);
                            }
                        } catch (\Exception $e) {
                            $this->logger->error('Error processing message', [
                                'message_id' => $messageId,
                                'topic' => $message->topic_name,
                                'error' => $e->getMessage()
                            ]);
                            
                            // 拒绝消息
                            $this->nack($message);
                        }
                        break;
                        
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                        $this->logger->debug('End of partition reached', [
                            'topic' => $message->topic_name,
                            'partition' => $message->partition,
                            'offset' => $message->offset
                        ]);
                        break;
                        
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        $this->logger->debug('Consumer timed out');
                        break;
                        
                    default:
                        $this->logger->error('Consumer error', [
                            'error' => rd_kafka_err2str($message->err)
                        ]);
                        break;
                }
            }
        } catch (\Exception $e) {
            $this->logger->error('Error in consumer', [
                'error' => $e->getMessage()
            ]);
            
            // 尝试重新连接
            $this->close();
            sleep(5);
            $this->consume($topics, $callback);
        }
    }
    
    /**
     * 确认消息已处理
     * 
     * @param mixed $message 消息对象
     * @return bool 确认结果
     */
    public function ack($message): bool
    {
        try {
            if ($this->consumer) {
                $this->consumer->commit($message);
                return true;
            }
            return false;
        } catch (\Exception $e) {
            $this->logger->error('Failed to commit message', [
                'error' => $e->getMessage()
            ]);
            return false;
        }
    }
    
    /**
     * 拒绝消息
     * 
     * @param mixed $message 消息对象
     * @param bool $requeue 是否重新入队（Kafka不支持，此参数被忽略）
     * @return bool 拒绝结果
     */
    public function nack($message, bool $requeue = false): bool
    {
        // Kafka没有直接的拒绝机制，我们可以选择不提交偏移量
        // 或者将偏移量提交到消息之前，这样消息会被重新消费
        $this->logger->warning('Message nacked (Kafka does not support direct nack)', [
            'topic' => $message->topic_name,
            'partition' => $message->partition,
            'offset' => $message->offset
        ]);
        
        return true;
    }
    
    /**
     * 关闭连接
     * 
     * @return void
     */
    public function close(): void
    {
        if ($this->consumer) {
            $this->consumer->close();
            $this->consumer = null;
        }
        
        $this->producer = null;
    }
    
    /**
     * 析构函数
     */
    public function __destruct()
    {
        $this->close();
    }
}