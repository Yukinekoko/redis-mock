package com.github.zxl0714.redismock.executor.hash;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/24
 */
public class HGETExecutor extends AbstractHashExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberFactor(params, 2);

        Map<Slice, Slice> map = getMap(base, params.get(0));
        Slice slice = map.get(params.get(1));
        if (slice == null) {
            return Response.NULL;
        }
        return Response.bulkString(slice);
    }
}
