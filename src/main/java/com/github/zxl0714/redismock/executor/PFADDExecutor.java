package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.InternalException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Set;

import static com.github.zxl0714.redismock.Utils.*;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class PFADDExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberGreater(params, 1);

        Slice key = params.get(0);
        Slice data = base.rawGet(key);
        boolean first;
        Set<Slice> set;
        int prev;
        if (data == null) {
            set = Sets.newHashSet();
            first = true;
            prev = 0;
        } else {
            try {
                set = deserializeObject(data);
            } catch (Exception e) {
                throw new WrongValueTypeException("WRONGTYPE Key is not a valid HyperLogLog string value.");
            }
            first = false;
            prev = set.size();
        }
        for(Slice v : params.subList(1, params.size())) {
            set.add(v);
        }
        try {
            Slice out = serializeObject(set);
            if (first) {
                base.rawPut(key, out, -1L);
            } else {
                base.rawPut(key, out, null);
            }
        } catch (Exception e) {
            throw new InternalException(e.getMessage());
        }
        if (prev != set.size()) {
            return Response.integer(1L);
        }
        return Response.integer(0L);

    }
}
