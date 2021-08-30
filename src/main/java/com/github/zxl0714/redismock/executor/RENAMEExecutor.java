package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public class RENAMEExecutor extends AbstractExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 2);

        Slice value = base.rawGet(params.get(0));
        if (value == null) {
            throw new WrongValueTypeException("bound must be positive");
        }
        if (params.get(1).equals(params.get(0))) {
            return Response.OK;
        }
        long deadLine = base.getDeadLine(params.get(0));
        base.rawPut(params.get(1), value, -1L);
        base.setDeadline(params.get(1), deadLine);
        base.del(params.get(0));
        return Response.OK;
    }
}
