package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.pattern.KeyPattern;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisBase {

    private final Map<Slice, Slice> base      = Maps.newHashMap();
    private final Map<Slice, Long>  deadlines = Maps.newHashMap();
    private       List<RedisBase>   syncBases = Lists.newArrayList();

    public RedisBase() {
    }

    public void addSyncBase(RedisBase base) {
        syncBases.add(base);
    }

    @Nullable
    public synchronized Slice rawGet(Slice key) {
        Preconditions.checkNotNull(key);

        Long deadline = deadlines.get(key);
        if (deadline != null && deadline != -1 && deadline <= System.currentTimeMillis()) {
            base.remove(key);
            deadlines.remove(key);
            return null;
        }
        return base.get(key);
    }

    public synchronized List<Slice> rawKeys(Slice pattern) {
        KeyPattern p = new KeyPattern(pattern);
        List<Slice> keys = new ArrayList<Slice>();
        for(Map.Entry<Slice, Slice> entry : base.entrySet()) {
            if(p.match(entry.getKey())){
                Long deadline = deadlines.get(entry.getKey());
                if (deadline != null && deadline != -1 && deadline <= System.currentTimeMillis()) {
                    base.remove(entry.getKey());
                    deadlines.remove(entry.getKey());
                }else {
                    keys.add(entry.getKey());
                }
            }
        }
        return keys;
    }

    @Nullable
    public synchronized Long getTTL(Slice key) {
        Preconditions.checkNotNull(key);

        Long deadline = deadlines.get(key);
        if (deadline == null) {
            return null;
        }
        if (deadline == -1) {
            return deadline;
        }
        long now = System.currentTimeMillis();
        if (now < deadline) {
            return deadline - now;
        }
        base.remove(key);
        deadlines.remove(key);
        return null;
    }

    public synchronized long setTTL(Slice key, long ttl) {
        Preconditions.checkNotNull(key);

        if (base.containsKey(key)) {
            deadlines.put(key, ttl + System.currentTimeMillis());
            for(RedisBase base : syncBases) {
                base.setTTL(key, ttl);
            }
            return 1L;
        }
        return 0L;
    }

    public synchronized long setDeadline(Slice key, long deadline) {
        Preconditions.checkNotNull(key);

        if (base.containsKey(key)) {
            deadlines.put(key, deadline);
            for(RedisBase base : syncBases) {
                base.setDeadline(key, deadline);
            }
            return 1L;
        }
        return 0L;
    }

    public synchronized void rawPut(Slice key, Slice value, @Nullable Long ttl) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);

        base.put(key, value);
        if (ttl != null) {
            if (ttl != -1) {
                deadlines.put(key, ttl + System.currentTimeMillis());
            } else {
                deadlines.put(key, -1L);
            }
        }
        for(RedisBase base : syncBases) {
            base.rawPut(key, value, ttl);
        }
    }

    public synchronized void del(Slice key) {
        Preconditions.checkNotNull(key);

        base.remove(key);
        deadlines.remove(key);

        for(RedisBase base : syncBases) {
            base.del(key);
        }
    }
}
