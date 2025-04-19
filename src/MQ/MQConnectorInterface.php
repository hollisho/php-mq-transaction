<?php

namespace hollisho\MQTransaction\MQ;

/**
 * 消息队列连接器接口
 * 
 * 定义消息队列连接器的标准接口
 */
interface MQConnectorInterface
{
    /**
     * 发送消息到队列
     * 
     * @param string $topic 消息主题
     * @param array $data 消息数据
     * @param string $messageId 消息ID
     * @param array $options 额外选项
     * @return bool 发送结果
     */
    public function send(string $topic, array $data, string $messageId, array $options = []): bool;
    
    /**
     * 消费消息
     * 
     * @param array $topics 要消费的主题列表
     * @param callable $callback 消息处理回调函数
     * @return void
     */
    public function consume(array $topics, callable $callback);
    
    /**
     * 确认消息已处理
     * 
     * @param mixed $message 消息对象
     * @return bool 确认结果
     */
    public function ack($message): bool;
    
    /**
     * 拒绝消息
     * 
     * @param mixed $message 消息对象
     * @param bool $requeue 是否重新入队
     * @return bool 拒绝结果
     */
    public function nack($message, bool $requeue = false): bool;
    
    /**
     * 关闭连接
     * 
     * @return void
     */
    public function close();
}