package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.*;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static com.github.zxl0714.redismock.Response.NULL;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class UNSUBSCRIBEExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        SocketAttributes socketAttributes = SocketContextHolder.getSocketAttributes();
        Preconditions.checkNotNull(socketAttributes);
        if (params.size() == 0) {
            base.unSubscribeAll(socketAttributes.getSocket());
        } else {
            for (Slice slice : params) {
                base.unSubscribe(slice, socketAttributes.getSocket());
            }
        }
        ImmutableList.Builder<Slice> builder = new ImmutableList.Builder<Slice>();
        builder.add(Response.bulkString(new Slice("unsubscribe")));
        builder.add(NULL);
        builder.add(Response.integer(0));
        return Response.array(builder.build());

    }
}
