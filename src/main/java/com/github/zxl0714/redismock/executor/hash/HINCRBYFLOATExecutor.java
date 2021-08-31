package com.github.zxl0714.redismock.executor.hash;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.Socket;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/25
 */
public class HINCRBYFLOATExecutor extends AbstractHashExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 3);

        Slice name = params.get(0);
        Slice key = params.get(1);
        Map<Slice, Slice> map = getMap(base, name);
        Slice value = map.get(key);
        double incr = getDouble(params.get(2));
        if (value != null) {
            double source = getDouble(value);
            incr += source;
        }
        if (incr == Double.NEGATIVE_INFINITY || incr == Double.POSITIVE_INFINITY) {
            throw new WrongValueTypeException("ERR increment would produce NaN or Infinity");
        }
        value = new Slice(Utils.formatDouble(incr));
        map.put(key, value);
        setMap(base, name, map);
        return Response.bulkString(value);
    }
}
