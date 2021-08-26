package com.github.zxl0714.redismock.executor.set;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/26
 */
public class SPOPExecutor extends AbstractSetExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 0);
        Utils.checkArgumentsNumberLess(params, 3);

        Set<Slice> set = getSet(base, params.get(0));
        Random random = new Random();
        if (params.size() == 1) {
            if (set.isEmpty()) {
                return Response.NULL;
            }
            int rd = random.nextInt(set.size());
            Iterator<Slice> iterator = set.iterator();
            Slice response = null;
            while (iterator.hasNext()) {
                Slice cur = iterator.next();
                if (rd-- == 0) {
                    iterator.remove();
                    response = cur;
                    break;
                }
            }
            setSet(base, params.get(0), set);
            return Response.bulkString(response);
        }
        long count = getLong(params.get(1));
        if (count < 0) {
            throw new WrongValueTypeException("ERR index out of range");
        }
        if (count == 0 || set.isEmpty()) {
            return Response.EMPTY_LIST;
        }
        if (count >= set.size()) {
            List<Slice> response = new ArrayList<>(set.size());
            for (Slice slice : set) {
                response.add(Response.bulkString(slice));
            }
            set.clear();
            setSet(base, params.get(0), set);
            return Response.array(response);
        }

        List<Slice> response = new ArrayList<>((int)count);
        while (count > 0) {
            int rd = random.nextInt(set.size());
            Iterator<Slice> iterator = set.iterator();
            while (iterator.hasNext()) {
                Slice slice = iterator.next();
                if (rd-- == 0) {
                    response.add(Response.bulkString(slice));
                    iterator.remove();
                    break;
                }

            }
            count--;
        }
        setSet(base, params.get(0), set);
        return Response.array(response);
    }
}
