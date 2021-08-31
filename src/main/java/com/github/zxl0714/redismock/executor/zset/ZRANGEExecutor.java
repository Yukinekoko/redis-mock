package com.github.zxl0714.redismock.executor.zset;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/31
 */
public class ZRANGEExecutor extends AbstractZSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 2);
        Utils.checkArgumentsNumberLess(params, 5);

        ZSet zSet = getZSet(base, params.get(0));
        int start = (int) getLong(params.get(1));
        int end = (int) getLong(params.get(2));
        boolean withScores = false;
        if (params.size() == 4) {
            if ("withscores".equals(params.get(3).toString())) {
                withScores = true;
            } else {
                throw new WrongValueTypeException("ERR syntax error");
            }
        }

        List<Map.Entry<Slice, Double>> list = range(zSet, start, end, false);
        List<Slice> response;
        if (withScores) {
            response = new ArrayList<>(list.size() * 2);
        } else {
            response = new ArrayList<>(list.size());
        }
        for (Map.Entry<Slice, Double> entry : list) {
            response.add(Response.bulkString(entry.getKey()));
            if (withScores) {
                response.add(Response.bulkString(new Slice(Utils.formatDouble(entry.getValue()))));
            }
        }
        return Response.array(response);
    }
}
