package com.github.zxl0714.redismock.executor.str;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.executor.AbstractExecutor;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.checkArgumentsNumberEquals;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class INCRBYExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberEquals(params, 2);

        try {
            Slice key = params.get(0);
            long d = Long.parseLong(String.valueOf(params.get(1)));
            Slice v = base.rawGet(key);
            if (v == null) {
                base.rawPut(key, new Slice(String.valueOf(d)), -1L);
                return Response.integer(d);
            }
            long r = Long.parseLong(new String(v.data())) + d;
            base.rawPut(key, new Slice(String.valueOf(r)), -1L);
            return Response.integer(r);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR value is not an integer or out of range");
        }

    }
}
