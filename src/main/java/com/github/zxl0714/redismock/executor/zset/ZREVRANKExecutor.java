package com.github.zxl0714.redismock.executor.zset;

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
 * @date 2021/8/31
 */
public class ZREVRANKExecutor extends AbstractZSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 2);

        ZSet zSet = getZSet(base, params.get(0));
        Slice key = params.get(1);
        int index = -1;
        List<Map.Entry<Slice, Double>> list = range(zSet, 0, -1, true);
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getKey().equals(key)) {
                index = i;
                break;
            }
        }
        if (index == -1 ) {
            return Response.NULL;
        }
        return Response.integer(index);
    }
}
