package com.github.zxl0714.redismock.executor.zset;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/31
 */
public class ZINCRBYExecutor extends AbstractZSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 3);

        ZSet zSet = getZSet(base, params.get(0));
        double incr = getDouble(params.get(1));
        double score = zSet.getOrDefault(params.get(2), 0D);
        score += incr;
        zSet.put(params.get(2), score);
        setZSet(base, params.get(0), zSet);
        return Response.bulkString(new Slice(Utils.formatDouble(score)));
    }
}
