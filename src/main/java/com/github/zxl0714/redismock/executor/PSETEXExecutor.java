package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.BaseException;
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
public class PSETEXExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberEquals(params, 3);

        try {
            long ttl = Long.parseLong(new String(params.get(1).data()));
            base.rawPut(params.get(0), params.get(2), ttl);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR value is not an integer or out of range");
        }
        return OK;
    }
}
