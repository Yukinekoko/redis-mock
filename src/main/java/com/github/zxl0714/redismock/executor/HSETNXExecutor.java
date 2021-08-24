package com.github.zxl0714.redismock.executor;

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
public class HSETNXExecutor extends AbstractHashExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 3);

        Slice name = params.get(0);
        Map<Slice, Slice> map = getMap(base, name);
        Slice key = params.get(1);
        Slice value = params.get(2);
        Slice source = map.putIfAbsent(key, value);
        if (source != null) {
            return Response.integer(0);
        }
        setMap(base, name, map);
        return Response.integer(1);
    }
}
