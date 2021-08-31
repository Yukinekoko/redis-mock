package com.github.zxl0714.redismock.executor.zset;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.InternalException;
import com.github.zxl0714.redismock.expecptions.WrongNumberOfArgumentsException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/31
 */
public class ZINTERSTOREExecutor extends AbstractZSetExecutor {

    private static final String AGGREGATE_SUM = "sum";

    private static final String AGGREGATE_MIN = "min";

    private static final String AGGREGATE_MAX = "max";

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberGreater(params, 2);

        Slice targetKey = params.get(0);
        int numKeys = (int) getLong(params.get(1));
        if (numKeys <= 0) {
            throw new WrongNumberOfArgumentsException();
        }
        List<Slice> keys = new ArrayList<>(numKeys);
        for (int i = 2; i < 2 + numKeys; i++) {
            if (i >= params.size()) {
                throw new WrongNumberOfArgumentsException();
            }
            keys.add(params.get(i));
        }

        int cur = 2 + numKeys;
        List<Double> weights = new ArrayList<>(numKeys);
        String aggregate = AGGREGATE_SUM;
        for (int i = cur; i <params.size(); i++) {
            Slice curParam = params.get(i);
            if ("weights".equals(curParam.toString())) {
                for (int wi = i + 1; wi < i + 1 + numKeys; wi++) {
                    if (wi >= params.size()) {
                        throw new WrongNumberOfArgumentsException();
                    }
                    weights.add(getDouble(params.get(wi)));
                }
                i += numKeys;
            } else if ("aggregate".equals(curParam.toString())) {
                if (i + 1 >= params.size()) {
                    throw new WrongNumberOfArgumentsException();
                }
                String type = params.get(i + 1).toString();
                if (AGGREGATE_MAX.equals(type)) {
                    aggregate = AGGREGATE_MAX;
                } else if (AGGREGATE_MIN.equals(type)) {
                    aggregate = AGGREGATE_MIN;
                } else if (AGGREGATE_SUM.equals(type)) {
                    aggregate = AGGREGATE_SUM;
                } else {
                    throw new WrongValueTypeException("ERR syntax error");
                }
                i++;
            } else {
                throw new WrongValueTypeException("ERR syntax error");
            }
        }
        ZSet destination;
        if (AGGREGATE_SUM.equals(aggregate)) {
            destination = interSum(keys, weights, base);
        } else if (AGGREGATE_MAX.equals(aggregate)) {
            destination = interMaxOrMin(keys, base, true);
        } else {
            destination = interMaxOrMin(keys, base, false);
        }

        if (destination.isEmpty()) {
            return Response.integer(0);
        }
        setZSet(base, targetKey, destination);
        return Response.integer(destination.size());
    }

    protected ZSet interSum(List<Slice> keys, List<Double> weights, RedisBase base) throws InternalException, WrongValueTypeException {
        if (keys.size() != weights.size() && !weights.isEmpty()) {
            throw new InternalException();
        }
        boolean customWeights = true;
        if (weights.isEmpty()) {
            customWeights = false;
        }
        ZSet destination = getZSet(base, keys.get(0));
        if (customWeights) {
            for (Map.Entry<Slice, Double> entry : destination.entrySet()) {
                entry.setValue(entry.getValue() * weights.get(0));
            }
        }
        for (int i = 1; i < keys.size(); i++) {
            ZSet processSet = getZSet(base, keys.get(i));
            double weight = customWeights ? weights.get(i) : 1D;
            Iterator<Map.Entry<Slice, Double>> it = destination.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<Slice, Double> dsEntry = it.next();
                if (processSet.containsKey(dsEntry.getKey())) {
                    double dsScore = dsEntry.getValue();
                    double pcScore = processSet.get(dsEntry.getKey()) * weight;
                    dsEntry.setValue(dsScore + pcScore);
                } else {
                    it.remove();
                }
            }
        }
        return destination;
    }

    protected ZSet interMaxOrMin(List<Slice> keys, RedisBase base, boolean max) throws WrongValueTypeException {
        ZSet destination = getZSet(base, keys.get(0));
        for (int i = 1; i < keys.size(); i++) {
            ZSet processSet = getZSet(base, keys.get(i));
            Iterator<Map.Entry<Slice, Double>> it = destination.entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<Slice, Double> dsEntry = it.next();
                if (processSet.containsKey(dsEntry.getKey())) {
                    double dsScore = dsEntry.getValue();
                    double pcScore = processSet.get(dsEntry.getKey());
                    if (max) {
                        dsEntry.setValue(Math.max(dsScore, pcScore));
                    } else {
                        dsEntry.setValue(Math.min(dsScore, pcScore));
                    }
                } else {
                    it.remove();
                }
            }
        }
        return destination;
    }


}
