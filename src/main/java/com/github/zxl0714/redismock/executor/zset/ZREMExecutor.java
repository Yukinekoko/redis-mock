package com.github.zxl0714.redismock.executor.zset;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/9/1
 */
public class ZREMExecutor extends AbstractZSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 1);

        ZSet zSet = getZSet(base, params.get(0));
        int response = 0;
        for (int i = 1; i <params.size(); i++) {
            Slice key = params.get(i);
            if (zSet.remove(key) != null) {
                response++;
            }
        }
        setZSet(base, params.get(0), zSet);
        return Response.integer(response);
    }
}
