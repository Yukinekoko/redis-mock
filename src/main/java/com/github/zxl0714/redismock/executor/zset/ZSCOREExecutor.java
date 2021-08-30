package com.github.zxl0714.redismock.executor.zset;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public class ZSCOREExecutor extends AbstractZSetExecutor {

    private static final DecimalFormat DF = new DecimalFormat("0.#################");

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 2);

        Map<Slice, Double> zSet = getZSet(base, params.get(0));
        Double score = zSet.get(params.get(1));
        if (score == null) {
            return Response.NULL;
        }
        return Response.bulkString(new Slice(DF.format(score)));
    }
}
