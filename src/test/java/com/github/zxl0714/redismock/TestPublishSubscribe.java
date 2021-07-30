package com.github.zxl0714.redismock;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author snowmeow(yuki754685421 @ yuki754685421.com)
 * @date 2021-7-20
 */
public class TestPublishSubscribe {

    private static RedisServer redisServer;

    private static AtomicInteger flag;

    @BeforeClass
    public static void init() throws IOException {
        redisServer = RedisServer.newRedisServer();
        redisServer.start();
    }

    @AfterClass
    public static void destroy() {
        redisServer.stop();
    }

    @Before
    public void initFlag() {
        flag = new AtomicInteger(0);
    }

    @Test(timeout = 5000)
    public void testPubSub() throws InterruptedException {
        Thread subscribeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                testSubscribe();
            }
        });
        Thread publishThread = new Thread(new Runnable() {
            @Override
            public void run() {
                testPublish();
            }
        });
        subscribeThread.start();
        Thread.sleep(1000);
        publishThread.start();
        publishThread.join();
        while (flag.get() <= 0) {
            Thread.sleep(500);
        }
    }

    @Test(timeout = 5000)
    public void testMultiplePubSub() throws InterruptedException {
        Thread subscribeThreadA = new Thread(new Runnable() {
            @Override
            public void run() {
                testMultipleSubscribe();
            }
        });
        Thread subscribeThreadB = new Thread(new Runnable() {
            @Override
            public void run() {
                testMultipleSubscribe();
            }
        });
        Thread publishThread = new Thread(new Runnable() {
            @Override
            public void run() {
                testMultiplePublish();
            }
        });
        subscribeThreadA.start();
        subscribeThreadB.start();
        Thread.sleep(1000);
        publishThread.start();
        publishThread.join();
        while (flag.get() <= 1) {
            Thread.sleep(500);
        }
    }

    public void testMultiplePublish() {
        Jedis jedis = new Jedis(redisServer.getHost(), redisServer.getBindPort());
        assertEquals(new Long(2), jedis.publish("channel_1", "Hello World"));
        assertEquals(new Long(0), jedis.publish("channel_2", "Hello World"));
        jedis.disconnect();
    }

    public void testMultipleSubscribe() {
        Jedis jedis = new Jedis(redisServer.getHost(), redisServer.getBindPort());
        jedis.subscribe(new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                assertEquals("channel_1", channel);
                assertEquals("Hello World", message);
                flag.incrementAndGet();
                unsubscribe();
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                assertEquals("channel_1", channel);
                assertEquals(1, subscribedChannels);
            }
        }, "channel_1");
        jedis.disconnect();
    }

    public void testPublish() {
        Jedis jedis = new Jedis(redisServer.getHost(), redisServer.getBindPort());
        assertEquals(new Long(1), jedis.publish("channel_1", "Hello World"));
        assertEquals(new Long(0), jedis.publish("channel_2", "Hello World"));
        jedis.disconnect();
    }

    public void testSubscribe() {
        final Jedis jedis = new Jedis(redisServer.getHost(), redisServer.getBindPort());
        jedis.subscribe(new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                assertEquals("Hello World", message);
                assertEquals("channel_1", channel);
                flag.incrementAndGet();
                unsubscribe();
            }

            @Override
            public void onSubscribe(String channel, int subscribedChannels) {
                assertEquals("channel_1", channel);
                assertEquals(1, subscribedChannels);
            }
        }, "channel_1");
        jedis.disconnect();
    }

}
