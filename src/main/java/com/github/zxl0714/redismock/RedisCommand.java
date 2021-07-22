package com.github.zxl0714.redismock;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisCommand {

    private List<Slice> params = Lists.newArrayList();

    public void addParameter(Slice token) {
        this.params.add(token);
    }

    public List<Slice> getParameters() {
        return params;
    }
}
