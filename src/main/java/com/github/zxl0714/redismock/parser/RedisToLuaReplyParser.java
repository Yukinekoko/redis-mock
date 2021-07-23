package com.github.zxl0714.redismock.parser;

import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.logging.Logger;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-23
 */
public class RedisToLuaReplyParser extends AbstractParser {

    private static final Logger LOGGER = Logger.getLogger(RedisCommandParser.class.getName());

    private RedisToLuaReplyParser(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * 将lua脚本中调用redis.call指令的响应转换为lua数据格式
     * @param slice 通过 CommandExecutor.execCommand得到的指令响应
     * @return LuaValue 转换后的Lua数据类型
     * */
    public static LuaValue parse(Slice slice) throws ParseErrorException {
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
        RedisToLuaReplyParser parser = new RedisToLuaReplyParser(input);
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
}
