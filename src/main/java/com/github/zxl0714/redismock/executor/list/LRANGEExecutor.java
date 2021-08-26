package com.github.zxl0714.redismock.executor.list;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.executor.AbstractExecutor;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.checkArgumentsNumberEquals;
import static com.github.zxl0714.redismock.Utils.deserializeObject;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class LRANGEExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberEquals(params, 3);

        Slice key = params.get(0);
        Slice data = base.rawGet(key);
        LinkedList<Slice> list;
        try {
            if (data != null) {
                list = deserializeObject(data);
            } else {
                list = Lists.newLinkedList();
            }
        } catch (Exception e) {
            throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        int start;
        int end;
        try {
            start = Integer.parseInt(params.get(1).toString());
            end = Integer.parseInt(params.get(2).toString());
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR value is not an integer or out of range");
        }
        if (start < 0) {
            start = list.size() + start;
            if (start < 0) {
                start = 0;
            }
        }
        if (end < 0) {
            end = list.size() + end;
            if (end < 0) {
                end = 0;
            }
        }
        ImmutableList.Builder<Slice> builder = new ImmutableList.Builder<>();
        for(int i = start; i <= end && i < list.size(); i++) {
            builder.add(Response.bulkString(list.get(i)));
        }
        return Response.array(builder.build());

    }
}
