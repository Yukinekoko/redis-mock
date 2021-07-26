package com.github.zxl0714.redismock.expecptions;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class RedisCallCommandException extends RuntimeException {

    public RedisCallCommandException() {
        super();
    }

    public RedisCallCommandException(String message) {
        super(message);
    }
}
