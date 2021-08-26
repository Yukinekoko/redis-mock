package com.github.zxl0714.redismock.executor.str;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.executor.AbstractExecutor;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.checkArgumentsNumberGreater;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class MGETExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberGreater(params, 0);

        ImmutableList.Builder<Slice> builder = new ImmutableList.Builder<Slice>();
        for(Slice key : params) {
            builder.add(Response.bulkString(base.rawGet(key)));

        }
        return Response.array(builder.build());

    }
}
