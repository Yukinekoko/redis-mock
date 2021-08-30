package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.Utils;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public class TYPEExecutor extends AbstractExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        Utils.checkArgumentsNumberEquals(params, 1);

        Slice value = base.rawGet(params.get(0));
        if (value == null) {
            return Response.NONE;
        }
        try {
            getList(value);
            return Response.bulkString(new Slice("list"));
        } catch (WrongValueTypeException ignored) {}
        try {
            getMap(value);
            return Response.bulkString(new Slice("hash"));
        } catch (WrongValueTypeException ignored) {}
        try {
            getSet(value);
            return Response.bulkString(new Slice("set"));
        } catch (WrongValueTypeException ignored) {}
        return Response.bulkString(new Slice("string"));
    }

    protected LinkedList<Slice> getList(Slice value) throws WrongValueTypeException {
        LinkedList<Slice> list;
        try {
            list = Utils.deserializeObject(value);
            return list;
        } catch (Exception e) {
            throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

    protected HashMap<Slice, Slice> getMap(Slice value) throws WrongValueTypeException {
        HashMap<Slice, Slice> map;
        try {
            map = Utils.deserializeObject(value);
            return map;
        } catch (Exception e) {
            throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

    protected HashSet<Slice> getSet(Slice value) throws WrongValueTypeException {
        HashSet<Slice> set;
        try {
            set = Utils.deserializeObject(value);
            return set;
        } catch (Exception e) {
            throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }
}
