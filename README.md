# redis-mock

A simple redis java mock for unit testing.

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.zxl0714/redis-mock/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.zxl0714/redis-mock)


```xml
<dependency>
  <groupId>com.github.zxl0714</groupId>
  <artifactId>redis-mock</artifactId>
  <version>0.1</version>
</dependency>
```

## How to use it

The very simple way.

```
private static RedisServer server = null;

@Before
public void before() throws IOException {
  server = RedisServer.newRedisServer();  // bind to a random port
}

@Test
public void test() {
  ...
  Jedis jedis = new Jedis(server.getHost(), server.getBindPort());
  ...
}

@After
public void after() {
  server.stop();
  server = null;
}
```

## Master and Slave

```
RedisServer master = newRedisServer();
RedisServer slave = newRedisServer();
master.setSlave(slave);
```

## Fault tolerance testing

We can make a RedisServer close connection after every serveral commands. This will cause a connection exception for clients.

```
RedisServer server = RedisServer.newRedisServer();
ServiceOptions options = new ServiceOptions();
options.setCloseSocketAfterSeveralCommands(3);
server.setOptions(options);
server.start();
```

## Support Commands

strlen

get

append

exists

set

ttl

decrby

pfadd

pfmerge

mget

mset

getset

del

expireat

pexpireat

setex

psetex

setnx

setbit

getbit

pttl

expire

pexpire

incr

incrby

decr

pfcount

keys

subscribe

unsubscribe

publish

select

ping

quit

eval (unfinished)

## eval
eval 指令尚未完成：

1. 在lua脚本中可以创建全局变量（在redis中执行的lua脚本不允许创建全局变量，会返回异常）
2. 在lua脚本中支持了package基础包的相关函数（在redis中执行的lua脚本没有引入package包）
3. 在lua脚本中尚未支持第三方包struct、cjson、bitop、cmsgpack
4. 暂不支持在lua脚本中调用redis.log指令

