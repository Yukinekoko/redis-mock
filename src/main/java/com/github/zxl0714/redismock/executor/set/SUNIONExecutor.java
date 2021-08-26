package com.github.zxl0714.redismock.executor.set;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/26
 */
public class SUNIONExecutor extends AbstractSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 0);

        Set<Slice> mainSet = getSet(base, params.get(0));
        for (int i = 1; i < params.size(); i++) {
            Set<Slice> curSet = getSet(base, params.get(i));
            mainSet.addAll(curSet);
        }
        List<Slice> response = new ArrayList<>(mainSet.size());
        for (Slice slice : mainSet) {
            response.add(Response.bulkString(slice));
        }
        return Response.array(response);
    }
}
