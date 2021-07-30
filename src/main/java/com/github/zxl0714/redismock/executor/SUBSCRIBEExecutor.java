package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.*;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.checkArgumentsNumberGreater;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class SUBSCRIBEExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberGreater(params, 0);
        SocketAttributes socketAttributes = SocketContextHolder.getSocketAttributes();
        Preconditions.checkNotNull(socketAttributes);
        ImmutableList.Builder<Slice> builder = new ImmutableList.Builder<Slice>();
        for (int i = 0; i < params.size(); i++) {
            builder.add(Response.bulkString(new Slice("subscribe")));
            builder.add(Response.bulkString(new Slice(params.get(i).toString())));
            builder.add(Response.integer(i + 1));
            base.subscribe(params.get(i), socketAttributes.getSocket());
        }
        return Response.array(builder.build());

    }
}
