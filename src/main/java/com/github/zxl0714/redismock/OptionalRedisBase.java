package com.github.zxl0714.redismock;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import java.util.List;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-19
 */
public class OptionalRedisBase extends RedisBase {

    private final RedisBase[] bases;

    public OptionalRedisBase() {
        this(16);
    }

    public OptionalRedisBase(int count) {
        bases = new RedisBase[count];
    }

    public int getBaseCount() {
        return bases.length;
    }

    @Override
    public void addSyncBase(RedisBase base) {
        selectBase().addSyncBase(base);
    }

    @Nullable
    @Override
    public synchronized Slice rawGet(Slice key) {
        return selectBase().rawGet(key);
    }

    @Override
    public synchronized List<Slice> rawKeys(Slice pattern) {
        return selectBase().rawKeys(pattern);
    }

    @Nullable
    @Override
    public synchronized Long getTTL(Slice key) {
        return selectBase().getTTL(key);
    }

    @Override
    public synchronized long setTTL(Slice key, long ttl) {
        return selectBase().setTTL(key, ttl);
    }

    @Override
    public synchronized long setDeadline(Slice key, long deadline) {
        return selectBase().setDeadline(key, deadline);
    }

    @Override
    public synchronized void rawPut(Slice key, Slice value, @Nullable Long ttl) {
        selectBase().rawPut(key, value, ttl);
    }

    @Override
    public synchronized void del(Slice key) {
        selectBase().del(key);
    }

    /**
     * 选择当前socket指定的数据库
     * */
    private synchronized RedisBase selectBase() {
        SocketAttributes socketAttributes = SocketContextHolder.getSocketAttributes();
        Preconditions.checkNotNull(socketAttributes);

        int index = socketAttributes.getDatabaseIndex();
        if (index >= bases.length) {
            index = 0;
        }
        RedisBase selectBase = bases[index];
        if (selectBase == null) {
            selectBase = new RedisBase();
            bases[index] = selectBase;
        }
        return selectBase;
    }
}
