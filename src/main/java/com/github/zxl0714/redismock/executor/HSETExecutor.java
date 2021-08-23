package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/23
 */
public class HSETExecutor extends AbstractHashExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 3);

        Slice key = params.get(0);
        HashMap<Slice, Slice> map = getMap(base, key);
        Slice old = map.put(params.get(1), params.get(2));
        setMap(base, key, map);
        if (old != null) {
            return Response.integer(0);
        }
        return Response.integer(1);
    }
}
