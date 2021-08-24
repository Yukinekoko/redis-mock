package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongNumberOfArgumentsException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/24
 */
public class HMSETExecutor extends AbstractHashExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 1);
        if ((params.size() - 1) % 2 != 0) {
            throw new WrongNumberOfArgumentsException();
        }
        Slice name = params.get(0);
        Map<Slice, Slice> map = getMap(base, name);
        for (int i = 1; i < params.size(); i++) {
            Slice key = params.get(i++);
            Slice value = params.get(i);
            map.put(key, value);
        }
        setMap(base, name, map);
        return Response.OK;
    }
}
