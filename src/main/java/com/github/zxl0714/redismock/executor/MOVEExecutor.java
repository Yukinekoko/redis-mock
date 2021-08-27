package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.*;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/27
 */
public class MOVEExecutor extends AbstractExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 2);

        Slice key = params.get(0);
        int targetIndex = (int) getLong(params.get(1));
        if (targetIndex < 0 || targetIndex >= base.getDataBaseCount()) {
            throw new WrongValueTypeException("ERR index out of range");
        }
        if (SocketContextHolder.getSocketAttributes().getDatabaseIndex() == targetIndex) {
            throw new WrongValueTypeException("source and destination objects are the same");
        }
        Slice source = base.rawGet(key);
        if (source == null) {
            return Response.integer(0);
        }
        Slice target = base.rawGet(key, targetIndex);
        if (target != null) {
            return Response.integer(0);
        }
        base.del(key);
        base.rawPut(key, source, -1L, targetIndex);
        return Response.integer(1);
    }
}
