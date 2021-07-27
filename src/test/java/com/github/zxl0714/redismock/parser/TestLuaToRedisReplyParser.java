package com.github.zxl0714.redismock.parser;

import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.github.zxl0714.redismock.parser.LuaToRedisReplyParser;
import com.github.zxl0714.redismock.parser.RedisToLuaReplyParser;
import org.junit.Test;
import org.luaj.vm2.Lua;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class TestLuaToRedisReplyParser {

    @Test
    public void testLuaToRedisParser() throws ParseErrorException {

        String strOK = "+OK\r\n";
        String strPONG = "+PONG\r\n";
        String strNULL = "$-1\r\n";
        String strError = "-ERR message\r\n";
        String strString1 = "$5\r\nhello\r\n";
        String strString2 = "$0\r\n";
        String strArray = "*4\r\n$5\r\nhello\r\n$-1\r\n:2\r\n$2\r\nab\r\n";
        String strNullArray = "*-1\r\n";
        String strNestArray = "*2\r\n:2\r\n*2\r\n$5\r\nhello\r\n:2\r\n";
        String strNumber = ":1\r\n";

        LuaValue luaOK =  new LuaTable();
        luaOK.set("ok", LuaValue.valueOf("OK"));
        LuaTable luaPONG = new LuaTable();
        luaPONG.set("ok", LuaValue.valueOf("PONG"));
        LuaValue luaNULL =  LuaValue.FALSE;
        LuaValue luaError =  new LuaTable();
        luaError.set("err", "ERR message");
        LuaValue luaString1 =  LuaValue.valueOf("hello");
        LuaValue luaString2 =  LuaValue.valueOf("");
        LuaValue luaNumber =  LuaValue.valueOf(1);
        LuaTable luaArray =  new LuaTable();
        luaArray.set(1, "hello");
        luaArray.set(2, LuaValue.FALSE);
        luaArray.set(3, LuaValue.valueOf(2));
        luaArray.set(4, LuaValue.valueOf("ab"));
        LuaValue luaNullArray = new LuaTable();
        LuaTable luaNestArray = new LuaTable();
        luaNestArray.set(1, LuaValue.valueOf(2));
        LuaTable t1 = new LuaTable();
        t1.set(1, "hello");
        t1.set(2, LuaValue.valueOf(2));
        luaNestArray.set(2, t1);

        assertEquals(strOK, LuaToRedisReplyParser.parse(luaOK).toString());
        assertEquals(strPONG, LuaToRedisReplyParser.parse(luaPONG).toString());
        assertEquals(strNULL, LuaToRedisReplyParser.parse(luaNULL).toString());
        assertEquals(strError, LuaToRedisReplyParser.parse(luaError).toString());
        assertEquals(strString1, LuaToRedisReplyParser.parse(luaString1).toString());
        assertEquals(strString2, LuaToRedisReplyParser.parse(luaString2).toString());
        assertEquals(strArray, LuaToRedisReplyParser.parse(luaArray).toString());
        assertEquals(strNullArray, LuaToRedisReplyParser.parse(luaNullArray).toString());
        assertEquals(strNestArray, LuaToRedisReplyParser.parse(luaNestArray).toString());
        assertEquals(strNumber, LuaToRedisReplyParser.parse(luaNumber).toString());
        assertEquals("$5\r\n00001\r\n", LuaToRedisReplyParser.parse(LuaValue.valueOf("00001")).toString());
        assertEquals(":-9223372036854775808\r\n", LuaToRedisReplyParser.parse(LuaValue.valueOf(-9223372036854775808L)).toString());
    }

    @Test
    public void testDoubleParser() throws ParseErrorException {
        LuaValue value = LuaValue.valueOf(3.6633);
        assertEquals(":3\r\n", LuaToRedisReplyParser.parse(value).toString());

    }

    @Test
    public void testArrayParser() throws ParseErrorException {
        LuaTable t1 = new LuaTable();
        t1.set(1, LuaValue.valueOf(1));
        t1.set(2, LuaValue.valueOf(22));
        assertEquals("*2\r\n:1\r\n:22\r\n", LuaToRedisReplyParser.parse(t1).toString());
        LuaTable t2 = new LuaTable();
        t2.set(1, LuaValue.valueOf(1));
        t2.set(2, LuaValue.NIL);
        t2.set(3, LuaValue.valueOf(33));
        assertEquals("*1\r\n:1\r\n", LuaToRedisReplyParser.parse(t2).toString());
        LuaTable t3 = new LuaTable();
        t3.set(1, LuaValue.valueOf(1));
        t3.set(LuaValue.valueOf("aaa"), LuaValue.valueOf("aaa"));
        t3.set(2, LuaValue.valueOf(33));
        assertEquals("*2\r\n:1\r\n:33\r\n", LuaToRedisReplyParser.parse(t3).toString());
    }

}
