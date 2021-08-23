package com.github.zxl0714.redismock;

import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/23
 */
public class TestRedissonConnect {

    @Test
    public void testLock() throws IOException {
        RedisServer server = RedisServer.newRedisServer();
        server.start();
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:" + server.getBindPort());
        RedissonClient client = Redisson.create(config);

        RLock lock = client.getLock("lock_a");
        lock.lock(2, TimeUnit.SECONDS);
        lock.unlock();
    }

}
