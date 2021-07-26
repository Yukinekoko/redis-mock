package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.InternalException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Set;

import static com.github.zxl0714.redismock.Response.OK;
import static com.github.zxl0714.redismock.Utils.*;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class PFMERGEExecutor extends AbstractExecutor {
    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberGreater(params, 0);

        Slice dst = params.get(0);
        Slice data = base.rawGet(dst);
        boolean first;
        Set<Slice> set;
        if (data == null) {
            set = Sets.newHashSet();
            first = true;
        } else {
            try {
                set = deserializeObject(data);
            } catch (Exception e) {
                throw new WrongValueTypeException("WRONGTYPE Key is not a valid HyperLogLog string value.");
            }
            first = false;
        }
        for(Slice v : params.subList(1, params.size())) {
            Slice src = base.rawGet(v);
            if (src != null) {
                try {
                    Set<Slice> s = deserializeObject(src);
                    set.addAll(s);
                } catch (Exception e) {
                    throw new WrongValueTypeException("WRONGTYPE Key is not a valid HyperLogLog string value.");
                }
            }
        }
        try {
            Slice out = serializeObject(set);
            if (first) {
                base.rawPut(dst, out, -1L);
            } else {
                base.rawPut(dst, out, null);
            }
        } catch (Exception e) {
            throw new InternalException(e.getMessage());
        }
        return OK;

    }
}
