package com.github.zxl0714.redismock.executor.str;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.executor.AbstractExecutor;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.checkArgumentsNumberGreater;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class SETExecutor extends AbstractExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberGreater(params, 1);
        Utils.checkArgumentsNumberLess(params, 6);

        Slice key = params.get(0);
        Slice value = params.get(1);

        int cur = 2;
        // nx xx
        String mode = null;
        // expire是否指定为时间戳
        boolean isTimestamp = false;
        long expire = -1L;
        while (cur < params.size()) {
            String param = params.get(cur).toString().toLowerCase();
            if (param.equals("nx") || param.equals("xx")) {
                if (mode != null && !mode.equals(param)) {
                    throw new WrongValueTypeException("ERR XX and NX options at the same time are not compatible");
                }
                mode = param;
            } else {
                cur++;
                if (cur >= params.size()) {
                    throw new WrongValueTypeException("ERR syntax error");
                }
                switch (param) {
                    case "ex":
                        expire = getLong(params.get(cur)) * 1000;
                        break;
                    case "px":
                        expire = getLong(params.get(cur));
                        break;
                    case "exat":
                        isTimestamp = true;
                        expire = getLong(params.get(cur)) * 1000;
                        break;
                    case "pxat":
                        expire = getLong(params.get(cur));
                        isTimestamp = true;
                        break;
                    default:
                        throw new WrongValueTypeException("ERR syntax error");
                }
            }
            cur++;
        }

        if ("nx".equals(mode)) {
            Slice source = base.rawGet(key);
            if (source != null) {
                return Response.NULL;
            }
        } else if ("xx".equals(mode)) {
            Slice source = base.rawGet(key);
            if (source == null) {
                return Response.NULL;
            }
        }
        if (isTimestamp) {
            base.rawPut(key, value, -1L);
            base.setDeadline(key, expire);
        } else {
            base.rawPut(key, value, expire);
        }
        return Response.OK;
    }
}
