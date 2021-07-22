package com.github.zxl0714.redismock.lua;

import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
// TODO : (snowmeow, 2021-7-22) 异常日志
/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-22
 */
public class ReplyConverter {

    private final InputStream input;

    public ReplyConverter(InputStream inputStream) {
        input = inputStream;
    }

    public static LuaValue redis2Lua(Slice slice) throws EOFException, ParseErrorException {
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
        ReplyConverter converter = new ReplyConverter(input);
        byte type = converter.consumeByte();
        if (type == (byte) '$') {
            long len = converter.consumeLong();
            converter.expectByte((byte) '\n');
            return LuaValue.valueOf(converter.consumeString(len));
        } else if (type == (byte) '-') {
            return LuaValue.valueOf(converter.consumeString());
        } else if (type == (byte) '*') {
            return converter.consumeArray();
        } else if (type == (byte) ':') {
            return LuaValue.valueOf(converter.consumeLong());
        }

        throw new ParseErrorException();
    }

    public static Slice lua2Redis(Varargs args) {
        return null;
    }

    private LuaValue consumeArray() throws ParseErrorException, EOFException {
        long size = consumeLong();
        byte type;
        expectByte((byte) '\n');


        LuaTable table = LuaValue.tableOf();
        for (int i = 0; i < size; i++) {
            type = consumeByte();
            LuaValue luaValue = null;
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
                if (b == '\n') {
                    break;
                }
                bo.write(b);
            } catch (EOFException e) {
                throw new ParseErrorException();
            }
        }
        return new String(bo.toByteArray());
    }

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

    private void expectByte(byte c) throws ParseErrorException, EOFException {
        if (consumeByte() != c) {
            throw new ParseErrorException();
        }
    }

    private byte consumeByte() throws EOFException {
        int b;
        try {
            b = input.read();
        } catch (IOException e) {
            throw new EOFException();
        }
        if (b == -1) {
            throw new EOFException();
        }
        return (byte) b;
    }

    private static boolean isNumber(byte c) {
        return '0' <= c && c <= '9';
    }

}
