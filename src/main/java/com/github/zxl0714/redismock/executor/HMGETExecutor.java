package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/23
 */
public class HMGETExecutor extends AbstractHashExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 1);

        Slice key = params.get(0);
        HashMap<Slice, Slice> map = getMap(base, key);
        List<Slice> response = new ArrayList<>(params.size() - 1);
        for (int i = 1; i < params.size(); i++) {
            Slice value = map.get(params.get(i));
            if (value == null) {
                response.add(Response.NULL);
            } else {
                response.add(Response.bulkString(value));
            }
        }
        return Response.array(response);
    }
}
