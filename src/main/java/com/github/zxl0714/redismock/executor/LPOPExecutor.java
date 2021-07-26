package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.InternalException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.*;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class LPOPExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberEquals(params, 1);

        Slice key = params.get(0);
        Slice data = base.rawGet(key);
        LinkedList<Slice> list;
        try {
            if (data != null) {
                list = deserializeObject(data);
            } else {
                return Response.NULL;
            }
        } catch (Exception e) {
            throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        if (list.isEmpty()) {
            return Response.NULL;
        }
        Slice v = list.removeFirst();
        try {
            base.rawPut(key, serializeObject(list), -1L);
        } catch (Exception e) {
            throw new InternalException(e.getMessage());
        }
        return Response.bulkString(v);
    }
}