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
 * @date 2021/8/30
 */
public class ZCOUNTExecutor extends AbstractZSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 3);

        ZSet zSet = getZSet(base, params.get(0));
        String paramMin = params.get(1).toString();
        String paramMax = params.get(2).toString();
        boolean oiMin = false;
        boolean oiMax = false;
        if (paramMin.charAt(0) == '(') {
            oiMin = true;
            paramMin = paramMin.substring(1);
        }
        if (paramMax.charAt(0) == '(') {
            oiMax = true;
            paramMax = paramMax.substring(1);
        }
        double max;
        double min;
        if (paramMin.equals("+inf")) {
            min = Double.POSITIVE_INFINITY;
        } else if (paramMin.equals("-inf")) {
            min = Double.NEGATIVE_INFINITY;
        } else {
            min = getDouble(new Slice(paramMin));
        }
        if (paramMax.equals("+inf")) {
            max = Double.POSITIVE_INFINITY;
        } else if (paramMax.equals("-inf")) {
            max = Double.NEGATIVE_INFINITY;
        } else {
            max = getDouble(new Slice(paramMax));
        }

        List<Map.Entry<Slice, Double>> list = count(zSet, min, max, oiMin, oiMax);
        return Response.integer(list.size());
    }
}
