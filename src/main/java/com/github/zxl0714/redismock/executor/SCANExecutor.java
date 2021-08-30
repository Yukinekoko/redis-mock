package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongNumberOfArgumentsException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;
import com.github.zxl0714.redismock.pattern.KeyPattern;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public class SCANExecutor extends AbstractExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        int paramsSize = params.size();
        if (paramsSize == 0 || paramsSize == 2 || paramsSize == 4 || paramsSize > 5) {
            throw new WrongNumberOfArgumentsException();
        }

        long flag = getLong(params.get(0));
        if (flag < 0) {
            throw new WrongValueTypeException("ERR syntax error");
        }
        Slice match = null;
        long count = 10L;
        if (params.size() >= 3) {
            Slice type = params.get(1);
            Slice val = params.get(2);
            if ("match".equalsIgnoreCase(type.toString())) {
                match = val;
            } else if ("count".equalsIgnoreCase(type.toString())) {
                count = getLong(val);
            } else {
                throw new WrongValueTypeException("ERR syntax error");
            }
        }
        if (params.size() >= 5) {
            Slice type = params.get(3);
            Slice val = params.get(4);
            if ("match".equalsIgnoreCase(type.toString())) {
                match = val;
            } else if ("count".equalsIgnoreCase(type.toString())) {
                count = getLong(val);
            } else {
                throw new WrongValueTypeException("ERR syntax error");
            }
        }if (count <= 0) {
            throw new WrongValueTypeException("ERR syntax error");
        }

        List<Slice> slices;
        slices = base.rawKeys();
        // 当前跳过数量
        long ignore = 0;
        // 当前取出数量
        long sum = 0;
        long returnFlag = 0;
        KeyPattern p = match == null ? null : new KeyPattern(match);
        List<Slice> list = new ArrayList<>((int)count);
        for (Slice slice : slices) {
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
