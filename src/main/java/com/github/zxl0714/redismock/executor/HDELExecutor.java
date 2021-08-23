package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/23
 */
public class HDELExecutor extends AbstractHashExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 1);

        Slice key = params.get(0);
        Map<Slice, Slice> map = getMap(base, key);
        int count = 0;
        for (int i = 1; i < params.size(); i++) {
            Slice cur = map.remove(params.get(i));
            if (cur != null) {
                count++;
            }
        }
        setMap(base, key, map);
        return Response.integer(count);
    }
}
