package com.github.zxl0714.redismock.executor.set;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/26
 */
public class SINTERSTOREExecutor extends AbstractSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 1);

        Slice name = params.get(0);
        List<Set<Slice>> setList = new ArrayList<>(params.size());
        Set<Slice> minSet = getSet(base, params.get(1));
        for (int i = 2; i < params.size(); i++) {
            Set<Slice> cur = getSet(base, params.get(i));
            if (cur.size() < minSet.size()) {
                setList.add(minSet);
                minSet = cur;
                continue;
            }
            setList.add(cur);
        }
        Iterator<Slice> iterator = minSet.iterator();
        while (iterator.hasNext()) {
            Slice curSlice = iterator.next();
            for (Set<Slice> curSet : setList) {
                if (!curSet.contains(curSlice)) {
                    iterator.remove();
                    break;
                }
            }
        }
        setSet(base, name, minSet);
        return Response.integer(minSet.size());
    }
}
