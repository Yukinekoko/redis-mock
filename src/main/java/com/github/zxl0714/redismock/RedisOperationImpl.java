package com.github.zxl0714.redismock;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/10/12
 */
public class RedisOperationImpl implements RedisOperation {

    private RedisBase redisBase;

    private RedisServer redisServer;

    public RedisOperationImpl(RedisBase redisBase, RedisServer redisServer) {
        this.redisBase = redisBase;
        this.redisServer = redisServer;
    }

    @Override
    public void clearAll() {
        redisBase.clearAll();
    }


}
