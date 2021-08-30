package com.github.zxl0714.redismock.executor.zset;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.executor.AbstractExecutor;
import com.github.zxl0714.redismock.expecptions.InternalException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.util.HashMap;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public abstract class AbstractZSetExecutor extends AbstractExecutor {

    public HashMap<Slice, Double> getZSet(RedisBase base, Slice key) throws WrongValueTypeException {
        Slice data = base.rawGet(key);
        HashMap<Slice, Double> zset;
        if (data != null) {
            try {
                zset = Utils.deserializeObject(data);
            } catch (Exception e) {
                throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
            }
        } else {
            zset = new HashMap<>();
        }
        return zset;
    }

    public void setZSet(RedisBase base, Slice key, HashMap<Slice, Double> zset) throws InternalException {
        try {
            base.rawPut(key, Utils.serializeObject(zset), -1L);
        } catch (Exception e) {
            throw new InternalException(e.getMessage());
        }
    }

    static final class Node {

        private Slice value;

        private double score;

        public Node() {
        }

        public Node(Slice value, double score) {
            this.value = value;
            this.score = score;
        }

        public Slice getValue() {
            return value;
        }

        public void setValue(Slice value) {
            this.value = value;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }

        @Override
        public int hashCode() {
            return value.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Node) {
                Node nodeObj = (Node) obj;
                return value.equals(nodeObj.getValue());
            }
            return false;
        }
    }

}
