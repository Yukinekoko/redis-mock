package com.github.zxl0714.redismock.executor.zset;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongNumberOfArgumentsException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/9/1
 */
public class ZREVRANGEBYSCOREExecutor extends AbstractZSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        if (params.size() != 3 && params.size() != 4 && params.size() != 6 && params.size() != 7) {
            throw new WrongNumberOfArgumentsException();
        }
        ZSet zSet = getZSet(base, params.get(0));
        String paramMin = params.get(2).toString();
        String paramMax = params.get(1).toString();
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

        boolean withScores = false;
        int offset = 0;
        int count = -1;
        for (int i = 3; i < params.size(); i++) {
            if ("withscores".equals(params.get(i).toString())) {
                withScores = true;
            } else if ("limit".equals(params.get(i).toString())) {
                if (i + 2 >= params.size()) {
                    throw new WrongValueTypeException("ERR syntax error");
                }
                offset = (int) getLong(params.get(i + 1));
                count = (int) getLong(params.get(i + 2));
                if (offset < 0) {
                    return Response.array(new ArrayList<>());
                }
                i += 2;
            } else {
                throw new WrongValueTypeException("ERR syntax error");
            }
        }

        List<Map.Entry<Slice, Double>> list = count(zSet, min, max, oiMin, oiMax);
        Collections.reverse(list);
        List<Slice> responseList = new ArrayList<>();
        for (int i = offset; i < list.size(); i++) {
            if (count-- == 0) {
                break;
            }
            responseList.add(Response.bulkString(list.get(i).getKey()));
            if (withScores) {
                responseList.add(
                    Response.bulkString(new Slice(Utils.formatDouble(list.get(i).getValue()))));
            }
        }
        return Response.array(responseList);
    }
}
