package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.*;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Random;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/27
 */
public class RANDOMKEYExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 0);

        List<Slice> keys = base.rawKeys();
        if (keys.isEmpty()) {
            return Response.NULL;
        }
        Random random = new Random();
        Slice key = keys.get(random.nextInt(keys.size()));
        return Response.bulkString(key);
    }
}
