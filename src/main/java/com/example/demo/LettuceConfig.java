package com.example.demo;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class LettuceConfig {

    @Value("${spring.redis.host}")
    private String redisHostname;
    @Value("${spring.redis.port}")
    private int redisPort;

    @Bean(destroyMethod = "shutdown")
    public RedisClient redisClient() {
        RedisURI redisURI = RedisURI.create(redisHostname,redisPort);
        return RedisClient.create(redisURI);
        //redisConnection = redisClient.connect();
    }

    @Bean(destroyMethod = "close")
    public StatefulRedisConnection<String, String> redisConnection(RedisClient client) {
        return client.connect();
    }

    @Bean
    public RedisCommands<String, String> redisCommands(StatefulRedisConnection<String, String> connection) {
        return connection.sync();
    }

    @Bean
    public RedisAsyncCommands<String, String> redisAsyncCommandsCommands(StatefulRedisConnection<String, String> connection) {
        return connection.async();
    }
}
