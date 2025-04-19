<?php

namespace hollisho\MQTransaction\MQ;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * RabbitMQ连接器
 * 
 * 实现基于RabbitMQ的消息队列连接
 */
class RabbitMQConnector implements MQConnectorInterface
{
    /**
     * @var array RabbitMQ连接配置
     */
    private $config;
    
    /**
     * @var AMQPStreamConnection|null RabbitMQ连接
     */
    private $connection;
    
    /**
     * @var \PhpAmqpLib\Channel\AMQPChannel|null RabbitMQ通道
     */
    private $channel;
    
    /**
     * @var LoggerInterface 日志记录器
     */
    private $logger;
    
    /**
     * @var array 交换机配置
     */
    private $exchanges = [];
    
    /**
     * @var array 队列配置
     */
    private $queues = [];
    
    /**
     * 构造函数
     * 
     * @param array $config RabbitMQ连接配置
     * @param LoggerInterface|null $logger 日志记录器
     */
    public function __construct(array $config, LoggerInterface $logger = null)
    {
        $this->config = array_merge([
            'host' => 'rabbitmq',
            'port' => 5672,
            'user' => 'myuser',
            'password' => 'mypass',
            'vhost' => '/',
        ], $config);
        
        $this->logger = $logger ?? new NullLogger();
    }
    
    /**
     * 声明交换机
     * 
     * @param string $name 交换机名称
     * @param string $type 交换机类型
     * @param bool $durable 是否持久化
     * @param bool $autoDelete 是否自动删除
     * @return self
     */
    public function declareExchange(string $name, string $type = 'topic', bool $durable = true, bool $autoDelete = false): self
    {
        $this->exchanges[$name] = [
            'type' => $type,
            'durable' => $durable,
            'auto_delete' => $autoDelete
        ];
        
        return $this;
    }
    
    /**
     * 声明队列
     * 
     * @param string $name 队列名称
     * @param bool $durable 是否持久化
     * @param bool $exclusive 是否排他
     * @param bool $autoDelete 是否自动删除
     * @return self
     */
    public function declareQueue(string $name, bool $durable = true, bool $exclusive = false, bool $autoDelete = false): self
    {
        $this->queues[$name] = [
            'durable' => $durable,
            'exclusive' => $exclusive,
            'auto_delete' => $autoDelete,
            'bindings' => []
        ];
        
        return $this;
    }
    
    /**
     * 绑定队列到交换机
     * 
     * @param string $queueName 队列名称
     * @param string $exchangeName 交换机名称
     * @param string $routingKey 路由键
     * @return self
     */
    public function bindQueue(string $queueName, string $exchangeName, string $routingKey): self
    {
        if (!isset($this->queues[$queueName])) {
            $this->declareQueue($queueName);
        }
        
        $this->queues[$queueName]['bindings'][] = [
            'exchange' => $exchangeName,
            'routing_key' => $routingKey
        ];
        
        return $this;
    }
    
    /**
     * 获取连接
     * 
     * @return AMQPStreamConnection
     */
    private function getConnection(): AMQPStreamConnection
    {
        if (!$this->connection) {
            $this->connection = new AMQPStreamConnection(
                $this->config['host'],
                $this->config['port'],
                $this->config['user'],
                $this->config['password'],
                $this->config['vhost']
            );
        }
        
        return $this->connection;
    }
    
    /**
     * 获取通道
     * 
     * @return \PhpAmqpLib\Channel\AMQPChannel
     */
    private function getChannel(): \PhpAmqpLib\Channel\AMQPChannel
    {
        if (!$this->channel) {
            $this->channel = $this->getConnection()->channel();
            
            // 声明交换机
            foreach ($this->exchanges as $name => $config) {
                $this->channel->exchange_declare(
                    $name,
                    $config['type'],
                    false,
                    $config['durable'],
                    $config['auto_delete']
                );
            }
            
            // 声明队列并绑定
            foreach ($this->queues as $name => $config) {
                $this->channel->queue_declare(
                    $name,
                    false,
                    $config['durable'],
                    $config['exclusive'],
                    $config['auto_delete']
                );
                
                // 绑定队列到交换机
                foreach ($config['bindings'] as $binding) {
                    $this->channel->queue_bind(
                        $name,
                        $binding['exchange'],
                        $binding['routing_key']
                    );
                }
            }
        }
        
        return $this->channel;
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
            $channel = $this->getChannel();
            
            // 默认交换机名称为topic
            $exchangeName = $options['exchange'] ?? $topic;
            
            // 确保交换机存在并声明
            $channel->exchange_declare(
                $exchangeName,
                'topic',
                false,
                true,
                false
            );
            
            // 创建消息属性
            $properties = [
                'content_type' => 'application/json',
                'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
                'message_id' => $messageId,
                'timestamp' => time(),
                'app_id' => $options['app_id'] ?? 'php-mq-transaction'
            ];
            
            // 添加自定义头信息
            if (isset($options['headers'])) {
                $properties['application_headers'] = $options['headers'];
            }
            
            // 创建消息
            $message = new AMQPMessage(
                json_encode($data),
                $properties
            );
            
            // 发送消息
            $channel->basic_publish(
                $message,
                $exchangeName,
                $topic
            );
            
            $this->logger->info('Message published', [
                'message_id' => $messageId,
                'topic' => $topic,
                'exchange' => $exchangeName
            ]);
            
            return true;
        } catch (\Exception $e) {
            $this->logger->error('Failed to publish message', [
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
    public function consume(array $topics, callable $callback)
    {
        try {
            $channel = $this->getChannel();
            
            // 为每个主题创建队列并绑定
            foreach ($topics as $topic) {
                $queueName = 'queue_' . str_replace('.', '_', $topic);
                
                // 声明队列
                if (!isset($this->queues[$queueName])) {
                    $this->declareQueue($queueName);
                }
                
                // 绑定队列到交换机
                $this->bindQueue($queueName, 'amq.topic', $topic);
                
                // 重新初始化通道以应用队列声明和绑定
                $channel = $this->getChannel();
                
                // 设置QoS
                $channel->basic_qos(null, 1, null);
                
                // 消费消息
                $channel->basic_consume(
                    $queueName,
                    '',
                    false,
                    false,
                    false,
                    false,
                    function ($message) use ($callback) {
                        try {
                            $body = json_decode($message->body, true);
                            $messageId = $message->get('message_id');
                            $topic = $message->delivery_info['routing_key'];
                            
                            $messageData = [
                                'message_id' => $messageId,
                                'topic' => $topic,
                                'data' => $body,
                                'raw_message' => $message
                            ];
                            
                            $result = $callback($messageData);
                            
                            if ($result) {
                                // 确认消息
                                $this->ack($message);
                            } else {
                                // 拒绝消息并重新入队
                                $this->nack($message, true);
                            }
                        } catch (\Exception $e) {
                            $this->logger->error('Error processing message', [
                                'error' => $e->getMessage()
                            ]);
                            
                            // 拒绝消息并重新入队
                            $this->nack($message, true);
                        }
                    }
                );
            }
            
            // 开始消费循环
            while ($channel->is_consuming()) {
                $channel->wait();
            }
        } catch (\Exception $e) {
            $this->logger->error('RabbitMQ consumer error', [
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
            $message->delivery_info['channel']->basic_ack(
                $message->delivery_info['delivery_tag']
            );
            return true;
        } catch (\Exception $e) {
            $this->logger->error('Failed to ack message', [
                'error' => $e->getMessage()
            ]);
            return false;
        }
    }
    
    /**
     * 拒绝消息
     * 
     * @param mixed $message 消息对象
     * @param bool $requeue 是否重新入队
     * @return bool 拒绝结果
     */
    public function nack($message, bool $requeue = false): bool
    {
        try {
            $message->delivery_info['channel']->basic_nack(
                $message->delivery_info['delivery_tag'],
                false,
                $requeue
            );
            return true;
        } catch (\Exception $e) {
            $this->logger->error('Failed to nack message', [
                'error' => $e->getMessage()
            ]);
            return false;
        }
    }
    
    /**
     * 关闭连接
     * 
     * @return void
     */
    public function close()
    {
        if ($this->channel) {
            $this->channel->close();
            $this->channel = null;
        }
        
        if ($this->connection) {
            $this->connection->close();
            $this->connection = null;
        }
    }
    
    /**
     * 析构函数
     */
    public function __destruct()
    {
        $this->close();
    }
}