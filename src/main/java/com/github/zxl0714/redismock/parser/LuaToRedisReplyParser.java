package com.github.zxl0714.redismock.parser;

import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-23
 */
public class LuaToRedisReplyParser {

    private static final Logger LOGGER = Logger.getLogger(LuaToRedisReplyParser.class.getName());

    /**
     * 将eval指令执行的lua脚本响应转换为redis数据格式
     * @param arg lua脚本的响应
     * @return 转换后的Redis响应
     * */
    public static Slice parse(LuaValue arg) throws ParseErrorException {
        if (arg.isnumber()) {
            return Response.integer(arg.toint());
        } else if (arg.isstring()) {
            String str = arg.tojstring();
            if (str.isEmpty()) {
                return Response.EMPTY_STRING;
            }
            return Response.bulkString(new Slice(str));
        } else if (arg.istable()) {
            return parseTable((LuaTable) arg);
        } else if (arg.isboolean()) {
            boolean flag = arg.toboolean();
            if (flag) {
                return Response.integer(1);
            } else {
                return Response.NULL;
            }
        }
        LOGGER.warning("parseLua2Redis error type, type is: " + arg.typename());
        throw new ParseErrorException();
    }

    private static Slice parseTable(LuaTable table) throws ParseErrorException {
        Map<Object, LuaValue> map = Maps.newHashMap();
        LuaValue k = LuaValue.NIL;
        while (true) {
            Varargs n = table.next(k);
            if ((k = n.arg1()).isnil())
                break;
            LuaValue v = n.arg(2);
            if (k.isint()) {
                map.put(k.toint(), v);
            } else if (k.isstring()) {
                map.put(k.tojstring(), v);
            } else {
                LOGGER.warning("lua table 中出现了错误的key格式： " + k.typename());
                throw new ParseErrorException();
            }
        }

        try {
            if (map.size() == 1 && map.get("ok") != null && !map.get("ok").isnil()) {
                return Response.status(table.get("ok").checkjstring());
            }
            if (map.size() == 1 && map.get("err") != null && !map.get("err").isnil()) {
                return Response.error(table.get("err").checkjstring());
            }
        }catch (LuaError e) {
            return Response.EMPTY_LIST;
        }

        List<Slice> responseList = Lists.newArrayList();
        for (int i = 1; map.get(i) != null; i++) {
            responseList.add(parse(map.get(i)));
        }
        if (responseList.isEmpty()) {
            return Response.EMPTY_LIST;
        }
        return Response.array(responseList);
    }

}
