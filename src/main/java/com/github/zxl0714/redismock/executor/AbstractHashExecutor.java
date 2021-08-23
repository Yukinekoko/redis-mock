package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.InternalException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/23
 */
public abstract class AbstractHashExecutor extends AbstractExecutor {

    public HashMap<Slice, Slice> getMap(RedisBase base, Slice key) throws WrongValueTypeException {
        Slice data = base.rawGet(key);
        HashMap<Slice, Slice> map;
        try {
            if (data != null) {
                map = Utils.deserializeObject(data);
            } else {
                map = new HashMap<>();
            }
        } catch (Exception e) {
            throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
        return map;
    }

    public void setMap(RedisBase base, Slice key, Map<Slice, Slice> map) throws InternalException {
        try {
            base.rawPut(key, Utils.serializeObject(map), -1L);
        } catch (Exception e) {
            throw new InternalException(e.getMessage());
        }
    }


}
