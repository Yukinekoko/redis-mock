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
public class SDIFFSTOREExecutor extends AbstractSetExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 1);

        Slice name = params.get(0);
        Set<Slice> set = getSet(base, params.get(1));
        for (int i = 2; i < params.size(); i++) {
            Set<Slice> diffSet = getSet(base, params.get(i));
            for (Slice slice : diffSet) {
                set.remove(slice);
            }
        }
        setSet(base, name, set);
        return Response.integer(set.size());
    }
}
