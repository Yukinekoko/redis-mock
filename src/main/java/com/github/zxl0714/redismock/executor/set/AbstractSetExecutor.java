package com.github.zxl0714.redismock.executor.set;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.executor.AbstractExecutor;
import com.github.zxl0714.redismock.expecptions.InternalException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.util.HashSet;
import java.util.Set;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/26
 */
public abstract class AbstractSetExecutor extends AbstractExecutor {

    public Set<Slice> getSet(RedisBase base, Slice key) throws WrongValueTypeException {
        Slice data = base.rawGet(key);
        HashSet<Slice> set;
        if (data != null) {
            try {
                set = Utils.deserializeObject(data);
            } catch (Exception e) {
                throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        } else {
            set = new HashSet<>();
        }
        return set;
    }

    public void setSet(RedisBase base, Slice key, Set<Slice> set) throws InternalException {
        try {
            base.rawPut(key, Utils.serializeObject(set), -1L);
        } catch (Exception e) {
            throw new InternalException(e.getMessage());
        }
    }
}
