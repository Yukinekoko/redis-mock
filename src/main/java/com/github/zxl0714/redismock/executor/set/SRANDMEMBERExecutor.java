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
import java.util.Random;
import java.util.Set;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/26
 */
public class SRANDMEMBERExecutor extends AbstractSetExecutor {

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
            for (Slice slice : set) {
                if (rd-- == 0) {
                    return Response.bulkString(slice);
                }
            }
        }
        long count = getLong(params.get(1));
        if (count < 0) {
            count = Math.abs(count);
        }
        if (count == 0 || set.isEmpty()) {
            return Response.EMPTY_LIST;
        }
        if (count >= set.size()) {
            List<Slice> response = new ArrayList<>(set.size());
            for (Slice slice : set) {
                response.add(Response.bulkString(slice));
            }
            return Response.array(response);
        }

        List<Slice> response = new ArrayList<>((int)count);
        while (count > 0) {
            int rd = random.nextInt(set.size());
            for (Slice slice : set) {
                if (rd-- == 0) {
                    response.add(Response.bulkString(slice));
                    break;
                }
            }
            count--;
        }
        return Response.array(response);
    }
}
