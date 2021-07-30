package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.checkArgumentsNumberEquals;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class APPENDExecutor extends AbstractExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberEquals(params, 2);

        Slice key = params.get(0);
        Slice value = params.get(1);
        Slice s = base.rawGet(key);
        if (s == null) {
            base.rawPut(key, value, -1L);
            return Response.integer(value.length());
        }
        byte[] b = new byte[s.length() + value.length()];
        for(int i = 0; i < s.length(); i++) {
            b[i] = s.data()[i];
        }
        for(int i = s.length(); i < s.length() + value.length(); i++) {
            b[i] = value.data()[i - s.length()];
        }
        base.rawPut(key, new Slice(b), -1L);
        return Response.integer(b.length);

    }
}
