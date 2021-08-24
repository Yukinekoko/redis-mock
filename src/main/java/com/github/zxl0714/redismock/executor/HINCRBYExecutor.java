package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/24
 */
public class HINCRBYExecutor extends AbstractHashExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 3);

        Map<Slice, Slice> map = getMap(base, params.get(0));
        Slice key = params.get(1);
        long incr;
        long response;
        try {
            incr = Long.parseLong(params.get(2).toString());
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR value is not an integer or out of range");
        }
        Slice value = map.get(key);
        if (value == null) {
            value = new Slice(String.valueOf(incr));
            response = incr;
        } else {
            long valueNum;
            try {
                valueNum = Long.parseLong(value.toString());
            } catch (NumberFormatException e) {
                throw new WrongValueTypeException("ERR hash value is not an integer");
            }

            try {
                response = Math.addExact(valueNum, incr);
            } catch (ArithmeticException e) {
                throw new WrongValueTypeException("ERR increment or decrement would overflow");
            }
            value = new Slice(String.valueOf(response));
        }
        map.put(key, value);
        setMap(base, params.get(0), map);
        return Response.integer(response);
    }
}
