package com.github.zxl0714.redismock.executor.hash;

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
 * @date 2021/8/24
 */
public class HLENExecutor extends AbstractHashExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 1);
        return Response.integer(getMap(base, params.get(0)).size());
    }
}
