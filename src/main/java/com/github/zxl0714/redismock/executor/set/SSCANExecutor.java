package com.github.zxl0714.redismock.executor.set;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;
import com.github.zxl0714.redismock.pattern.KeyPattern;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/26
 */
public class SSCANExecutor extends AbstractSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 1);
        Utils.checkArgumentsNumberLess(params, 7);
        Utils.checkArgumentsNumberFactor(params, 2);

        Slice name = params.get(0);
        long flag = getLong(params.get(1));
        if (flag < 0) {
            throw new WrongValueTypeException("ERR syntax error");
        }
        Slice match = null;
        long count = 10L;
        if (params.size() >= 4) {
            Slice type = params.get(2);
            Slice val = params.get(3);
            if ("match".equalsIgnoreCase(type.toString())) {
                match = val;
            } else if ("count".equalsIgnoreCase(type.toString())) {
                count = getLong(val);
            } else {
                throw new WrongValueTypeException("ERR syntax error");
            }
        }
        if (params.size() >= 6) {
            Slice type = params.get(4);
            Slice val = params.get(5);
            if ("match".equalsIgnoreCase(type.toString())) {
                match = val;
            } else if ("count".equalsIgnoreCase(type.toString())) {
                count = getLong(val);
            } else {
                throw new WrongValueTypeException("ERR syntax error");
            }
        }
        if (count <= 0) {
            throw new WrongValueTypeException("ERR syntax error");
        }
        Set<Slice> set = getSet(base, name);
        // 当前跳过数量
        long ignore = 0;
        // 当前取出数量
        long sum = 0;
        long returnFlag = 0;
        KeyPattern p = match == null ? null : new KeyPattern(match);
        List<Slice> list = new ArrayList<>((int)(count));
        for (Slice slice : set) {
            if (ignore < flag) {
                ignore++;
                continue;
            }
            if (sum == count) {
                returnFlag = flag + count;
                break;
            }
            sum++;
            if (p != null && !p.match(slice)) {
                continue;
            }
            list.add(Response.bulkString(slice));
        }

        List<Slice> response = new ArrayList<>(2);
        response.add(Response.bulkString(new Slice(String.valueOf(returnFlag))));
        if (list.isEmpty()) {
            response.add(Response.EMPTY_LIST);
        } else {
            response.add(Response.array(list));
        }
        return Response.array(response);
    }
}
