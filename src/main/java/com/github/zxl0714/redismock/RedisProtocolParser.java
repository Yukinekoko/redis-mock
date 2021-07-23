package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisProtocolParser {

    private static final Logger LOGGER = Logger.getLogger(RedisProtocolParser.class.getName());

    private final InputStream messageInput;

    @VisibleForTesting
    RedisProtocolParser(String stringInput) {
        this(new ByteArrayInputStream(stringInput.getBytes()));
    }

    @VisibleForTesting
    RedisProtocolParser(InputStream messageInput) {
        Preconditions.checkNotNull(messageInput);

        this.messageInput = messageInput;
    }

    @VisibleForTesting
    byte consumeByte() throws EOFException {
        int b;
        try {
            b = messageInput.read();
        } catch (IOException e) {
            throw new EOFException();
        }
        if (b == -1) {
            throw new EOFException();
        }
        return (byte) b;
    }

    @VisibleForTesting
    void expectByte(byte c) throws ParseErrorException, EOFException {
        if (consumeByte() != c) {
            throw new ParseErrorException();
        }
    }

    /**
     * 读取的byte中只能出现数字，在parseCommand()中使用
     * */
    @VisibleForTesting
    long consumePositiveLong() throws ParseErrorException {
        byte c;
        long ret = 0;
        boolean hasLong = false;
        while (true) {
            try {
                c = consumeByte();
            } catch (EOFException e) {
                throw new ParseErrorException();
            }
            if (c == '\r') {
                break;
            }
            if (!isNumber(c)) {
                throw new ParseErrorException();
            }
            ret = ret * 10 + c - '0';
            hasLong = true;
        }
        if (!hasLong) {
            throw new ParseErrorException();
        }
        return ret;
    }

    /**
     * 读取的byte中只能出现数字或是负号'-'，在parseRedis2Lua()中使用
     * */
    private long consumeLong() throws ParseErrorException {
        byte c;
        long ret = 0;
        boolean hasLong = false;
        boolean isMinus = false;
        while (true) {
            try {
                c = consumeByte();
            } catch (EOFException e) {
                throw new ParseErrorException();
            }
            if (c == '\r') {
                break;
            }
            if (c == '-') {
                isMinus = true;
                continue;
            }
            if (!isNumber(c)) {
                throw new ParseErrorException();
            }
            ret = ret * 10 + c - '0';
            hasLong = true;
        }
        if (!hasLong) {
            throw new ParseErrorException();
        }
        if (isMinus) {
            ret = -ret;
        }
        return ret;
    }

    @VisibleForTesting
    Slice consumeSlice(long len) throws ParseErrorException {
        ByteArrayDataOutput bo = ByteStreams.newDataOutput();
        for (long i = 0; i < len; i++) {
            try {
                bo.write(consumeByte());
            } catch (EOFException e) {
                throw new ParseErrorException();
            }
        }
        return new Slice(bo.toByteArray());
    }

    @VisibleForTesting
    long consumeCount() throws ParseErrorException, EOFException {
        expectByte((byte) '*');
        try {
            long count = consumePositiveLong();
            expectByte((byte) '\n');
            return count;
        } catch (EOFException e) {
            throw new ParseErrorException();
        }
    }

    @VisibleForTesting
    Slice consumeParameter() throws ParseErrorException {
        try {
            expectByte((byte) '$');
            long len = consumePositiveLong();
            expectByte((byte) '\n');
            Slice para = consumeSlice(len);
            expectByte((byte) '\r');
            expectByte((byte) '\n');
            return para;
        } catch (EOFException e) {
            throw new ParseErrorException();
        }
    }

    private static boolean isNumber(byte c) {
        return '0' <= c && c <= '9';
    }

    private String consumeString(long len) throws ParseErrorException {
        ByteArrayDataOutput bo = ByteStreams.newDataOutput();
        for (long i = 0; i < len; i++) {
            try {
                bo.write(consumeByte());
            } catch (EOFException e) {
                throw new ParseErrorException();
            }
        }
        return new String(bo.toByteArray());
    }

    private String consumeString() throws ParseErrorException {
        ByteArrayDataOutput bo = ByteStreams.newDataOutput();
        while(true) {
            try {
                byte b = consumeByte();
                if (b == '\r') {
                    break;
                }
                bo.write(b);
            } catch (EOFException e) {
                throw new ParseErrorException();
            }
        }
        return new String(bo.toByteArray());
    }

    private LuaValue consumeArray() throws ParseErrorException, EOFException {
        long size = consumeLong();
        byte type;
        expectByte((byte) '\n');


        LuaTable table = LuaValue.tableOf();
        for (int i = 0; i < size; i++) {
            type = consumeByte();
            LuaValue luaValue;
            if (type == (byte) '$') {
                long len = consumeLong();
                expectByte((byte) '\n');
                if (len == -1) {
                    table.set(i + 1, LuaValue.valueOf(false));
                    continue;
                }
                String result = consumeString(len);
                expectByte((byte) '\r');
                expectByte((byte) '\n');
                luaValue = LuaValue.valueOf(result);
            } else if (type == (byte) '-') {
                String result = consumeString();
                expectByte((byte) '\n');
                LuaValue[] entry = new LuaValue[2];
                entry[0] = LuaValue.valueOf("err");
                entry[1] = LuaValue.valueOf(result);
                luaValue = LuaValue.tableOf(entry);
            } else if (type == (byte) '*') {
                luaValue = consumeArray();
            } else if (type == (byte) ':') {
                long result = consumeLong();
                expectByte((byte) '\n');
                luaValue =LuaValue.valueOf(result);
            } else {
                throw new ParseErrorException();
            }
            table.set(i + 1, luaValue);
        }

        return table;
    }

    @VisibleForTesting
    static RedisCommand parseCommand(String stringInput) throws ParseErrorException, EOFException {
        Preconditions.checkNotNull(stringInput);

        return parseCommand(new ByteArrayInputStream(stringInput.getBytes()));
    }

    public static RedisCommand parseCommand(InputStream messageInput) throws ParseErrorException, EOFException {
        Preconditions.checkNotNull(messageInput);

        RedisProtocolParser parser = new RedisProtocolParser(messageInput);
        long count = parser.consumeCount();
        if (count == 0) {
            throw new ParseErrorException();
        }
        RedisCommand command = new RedisCommand();
        for (long i = 0; i < count; i++) {
            command.addParameter(parser.consumeParameter());
        }
        return command;
    }

    /**
     * 将lua脚本中调用redis.call指令的响应转换为lua数据格式
     * @param slice 通过 CommandExecutor.execCommand得到的指令响应
     * @return LuaValue 转换后的Lua数据类型
     * */
    public static LuaValue parseRedis2Lua(Slice slice) throws ParseErrorException {
        if (slice.equals(Response.OK)) {
            LuaTable table = LuaValue.tableOf();
            table.set("ok", LuaValue.valueOf("OK"));
            return table;
        } else if (slice.equals(Response.PONG)) {
            LuaTable table = LuaValue.tableOf();
            table.set("ok", LuaValue.valueOf("PONG"));
            return table;
        } else if (slice.equals(Response.NULL)) {
            return LuaValue.valueOf(false);
        }

        ByteArrayInputStream input = new ByteArrayInputStream(slice.data());
        RedisProtocolParser parser = new RedisProtocolParser(input);
        byte type;
        try {
            type = parser.consumeByte();
            if (type == (byte) '$') {
                long len = parser.consumeLong();
                parser.expectByte((byte) '\n');
                return LuaValue.valueOf(parser.consumeString(len));
            } else if (type == (byte) '-') {
                String result = parser.consumeString();
                LuaValue[] entry = new LuaValue[2];
                entry[0] = LuaValue.valueOf("err");
                entry[1] = LuaValue.valueOf(result);
                return LuaValue.tableOf(entry);
            } else if (type == (byte) '*') {
                return parser.consumeArray();
            } else if (type == (byte) ':') {
                return LuaValue.valueOf(parser.consumeLong());
            }
        } catch (EOFException | ParseErrorException e) {
            LOGGER.warning("parse error:" + slice.toString());
            throw new ParseErrorException();
        }
        LOGGER.warning("parse error:" + slice.toString());
        throw new ParseErrorException();
    }

    /**
     * 将eval指令执行的lua脚本响应转换为redis数据格式
     * @param arg lua脚本的响应
     * @return 转换后的Redis响应
     * */
    public static Slice parseLua2Redis(LuaValue arg) throws ParseErrorException {
        if (arg.isnumber()) {
            return Response.integer(arg.toint());
        } else if (arg.isstring()) {
            String str = arg.tojstring();
            if (str.isEmpty()) {
                return Response.EMPTY_STRING;
            }
            return Response.bulkString(new Slice(str));
        } else if (arg.istable()) {
            return parseLuaTable((LuaTable) arg);
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

    private static Slice parseLuaTable(LuaTable table) throws ParseErrorException {
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
            responseList.add(parseLua2Redis(map.get(i)));
        }
        if (responseList.isEmpty()) {
            return Response.EMPTY_LIST;
        }
        return Response.array(responseList);
    }
}
