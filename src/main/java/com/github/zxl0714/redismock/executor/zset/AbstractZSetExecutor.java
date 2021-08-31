package com.github.zxl0714.redismock.executor.zset;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.executor.AbstractExecutor;
import com.github.zxl0714.redismock.expecptions.InternalException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.util.*;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public abstract class AbstractZSetExecutor extends AbstractExecutor {

    // private Comparator<Map.Entry<Slice, Double>> cmp = (o1, o2) -> (int) (o1.getValue() - o2.getValue());

    private final Comparator<Map.Entry<Slice, Double>> cmp = (o1, o2) -> {
        if (o1.getValue() > o2.getValue()) {
            return 1;
        } else if (o1.getValue() < o2.getValue()) {
            return -1;
        } else {
            return o1.getKey().compareTo(o2.getKey());
        }
    };

    private final Comparator<Map.Entry<Slice, Double>> revCmp = (o1, o2) -> {
        if (o1.getValue() > o2.getValue()) {
            return -1;
        } else if (o1.getValue() < o2.getValue()) {
            return 1;
        } else {
            return o2.getKey().compareTo(o1.getKey());
        }
    };

    public ZSet getZSet(RedisBase base, Slice key) throws WrongValueTypeException {
        Slice data = base.rawGet(key);
        ZSet zset;
        if (data != null) {
            try {
                zset = Utils.deserializeObject(data);
            } catch (Exception e) {
                throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        } else {
            zset = new ZSet();
        }
        return zset;
    }

    public void setZSet(RedisBase base, Slice key, ZSet zset) throws InternalException {
        try {
            base.rawPut(key, Utils.serializeObject(zset), -1L);
        } catch (Exception e) {
            throw new InternalException(e.getMessage());
        }
    }

    public List<Map.Entry<Slice, Double>> count(ZSet zSet, double min, double max, boolean opMin, boolean opMax) {
        List<Map.Entry<Slice, Double>> list = new ArrayList<>(zSet.entrySet());
        list.sort(cmp);
        int start = -1;
        int end = -1;
        for (int i = 0; i < list.size(); i++) {
            if (list.get(i).getValue() > min && start == -1) {
                start = i;
            }
            if (list.get(i).getValue() == min && start == -1 && !opMin) {
                start = i;
            }
            if (list.get(i).getValue() > max) {
                break;
            }
            if (list.get(i).getValue() == max && opMax) {
                break;
            }
            end = i;
        }
        if (start == -1 || end == -1) {
            return new ArrayList<>();
        }
        return list.subList(start, end + 1);
    }

    public List<Map.Entry<Slice, Double>> range(ZSet zSet, int start, int end, boolean rev) {
        if (end >= zSet.size()) {
            end = zSet.size() - 1;
        }
        if (start < 0) {
            start = zSet.size() + start;
        }
        if (end < 0) {
            end = zSet.size() + end;
        }
        if (start > end || end < 0) {
            return new ArrayList<>();
        }
        if (start < 0) {
            start = 0;
        }

        List<Map.Entry<Slice, Double>> list = new ArrayList<>(zSet.entrySet());
        if (rev) {
            list.sort(revCmp);
        } else {
            list.sort(cmp);
        }
        return list.subList(start, end + 1);
    }

    static final class ZSet extends HashMap<Slice, Double> {

    }

}
