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
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public class ZADDExecutor extends AbstractZSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params,2);

        Slice name = params.get(0);
        ZSet zSet = getZSet(base, name);

        int cur = 1;
        String mode = null;
        boolean ch = false;
        boolean incr = false;
        while (cur < params.size()) {
            String param = params.get(cur).toString();
            if (param.equals("nx") || param.equals("xx")) {
                if (mode != null && !mode.equals(param)) {
                    throw new WrongValueTypeException("ERR XX and NX options at the same time are not compatible");
                }
                mode = param;
            } else if (param.equals("ch")) {
                ch = true;
            } else if (param.equals("incr")) {
                incr = true;
            } else {
                break;
            }
            cur++;
        }
        if (cur + 1 >= params.size()) {
            throw new WrongNumberOfArgumentsException();
        }
        if (incr && cur + 2 != params.size()) {
            throw new WrongValueTypeException("ERR INCR option supports a single increment-element pair");
        }

        double incrResponse = 0;
        int response = 0;
        for (int i = cur; i < params.size(); i += 2) {
            if (i + 1 >= params.size()) {
                throw new WrongNumberOfArgumentsException();
            }
            double score = getDouble(params.get(i));
            Slice value = params.get(i + 1);
            if ("xx".equals(mode) && !zSet.containsKey(value)) {
                continue;
            } else if ("nx".equals(mode) && zSet.containsKey(value)) {
                continue;
            }
            if (incr) {
                score += zSet.getOrDefault(value, 0D);
                incrResponse = score;
                zSet.put(value, score);
                break;
            }
            Double old = zSet.put(value, score);
            if (old != null && !ch) {
                continue;
            }
            response++;
        }
        setZSet(base, name, zSet);
        if (incr) {
            return Response.bulkString(new Slice(Utils.formatDouble(incrResponse)));
        }
        return Response.integer(response);
    }
}
