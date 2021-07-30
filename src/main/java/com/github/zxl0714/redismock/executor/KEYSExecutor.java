package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.checkArgumentsNumberEquals;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class KEYSExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberEquals(params, 1);
        Slice pattern = params.get(0);
        List<Slice> keys = base.rawKeys(pattern);
        ImmutableList.Builder<Slice> builder = new ImmutableList.Builder<>();
        for(Slice key : keys) {
            builder.add(Response.bulkString(key));
        }
        return Response.array(builder.build());

    }
}
