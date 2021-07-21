package com.github.zxl0714.redismock;

import com.google.common.collect.Maps;

import java.util.*;

/**
 * @author snowmeow(snowmeow @ yuki754685421.com)
 */
public class RedisDataBase {

    private final Map<Slice, Slice> base = Maps.newHashMap();

    private final Map<Slice, Long> deadlines = Maps.newHashMap();

    public Map<Slice, Slice> getBase() {
        return base;
    }

    public Map<Slice, Long> getDeadlines() {
        return deadlines;
    }
}
