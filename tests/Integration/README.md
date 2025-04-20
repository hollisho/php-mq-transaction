## 如何运行测试
要运行这个集成测试，你需要确保有可用的 MySQL 数据库和 RabbitMQ 服务
### 设置相关环境变量：

```shell
export RUN_INTEGRATION_TESTS=1
export DB_DSN="mysql:host=localhost;dbname=test_db;charset=utf8mb4"
export DB_USER="your_db_user"
export DB_PASSWORD="your_db_password"
export MQ_HOST="localhost"
export MQ_PORT=5672
export MQ_USER="guest"
export MQ_PASSWORD="guest"
export MQ_VHOST="/"
```

### 运行测试：

```shell
vendor/bin/phpunit tests/Integration/OrderServiceIntegrationTest.php
```