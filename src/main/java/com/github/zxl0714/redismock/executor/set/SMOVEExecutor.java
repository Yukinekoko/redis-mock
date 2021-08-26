package com.github.zxl0714.redismock.executor.set;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Set;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/26
 */
public class SMOVEExecutor extends AbstractSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 3);

        Set<Slice> source = getSet(base, params.get(0));
        Set<Slice> destination = getSet(base, params.get(1));
        Slice members = params.get(2);
        if (!source.remove(members)) {
            return Response.integer(0);
        }
        destination.add(members);
        setSet(base, params.get(0), source);
        setSet(base, params.get(1), destination);
        return Response.integer(1);
    }
}
