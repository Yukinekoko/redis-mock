package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.*;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongNumberOfArgumentsException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static com.github.zxl0714.redismock.Response.OK;
import static com.github.zxl0714.redismock.Utils.checkArgumentsNumberEquals;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class SELECTExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberEquals(params, 1);
        int index;
        try {
            index = Integer.parseInt(params.get(0).toString());
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR value is not an integer or out of range");
        }
        if (index >= base.getDataBaseCount() || index < 0) {
            return Response.error("ERR DB index is out of range");
        }
        SocketAttributes socketAttributes = SocketContextHolder.getSocketAttributes();
        socketAttributes.setDatabaseIndex(index);
        return OK;

    }
}
