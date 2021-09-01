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
 * @date 2021/9/1
 */
public class ZREMRANGEBYRANKExecutor extends AbstractZSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 3);

        ZSet zSet = getZSet(base, params.get(0));
        int start = (int) getLong(params.get(1));
        int end = (int) getLong(params.get(2));
        List<Map.Entry<Slice, Double>> list = range(zSet, start, end, false);
        for (Map.Entry<Slice, Double> entry : list) {
            zSet.remove(entry.getKey());
        }
        setZSet(base, params.get(0), zSet);
        return Response.integer(list.size());
    }
}
