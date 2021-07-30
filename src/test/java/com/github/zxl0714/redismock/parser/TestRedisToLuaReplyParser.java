package com.github.zxl0714.redismock.parser;

import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.github.zxl0714.redismock.parser.LuaToRedisReplyParser;
import com.github.zxl0714.redismock.parser.RedisToLuaReplyParser;
import org.junit.Test;
import org.luaj.vm2.LuaValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class TestRedisToLuaReplyParser {

    @Test
    public void testParseRedis2Lua() throws ParseErrorException {

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

        LuaValue luaOK =  RedisToLuaReplyParser.parse(new Slice(strOK));
        LuaValue luaPONG =  RedisToLuaReplyParser.parse(new Slice(strPONG));
        LuaValue luaNULL =  RedisToLuaReplyParser.parse(new Slice(strNULL));
        LuaValue luaError =  RedisToLuaReplyParser.parse(new Slice(strError));
        LuaValue luaString1 =  RedisToLuaReplyParser.parse(new Slice(strString1));
        LuaValue luaString2 =  RedisToLuaReplyParser.parse(new Slice(strString2));
        LuaValue luaNumber =  RedisToLuaReplyParser.parse(new Slice(strNumber));
        LuaValue luaArray =  RedisToLuaReplyParser.parse(new Slice(strArray));
        LuaValue luaNullArray =  RedisToLuaReplyParser.parse(new Slice(strNullArray));
        LuaValue luaNestArray =  RedisToLuaReplyParser.parse(new Slice(strNestArray));

        assertEquals("OK", luaOK.get("ok").checkjstring());
        assertEquals("PONG", luaPONG.get("ok").checkjstring());
        assertFalse(luaNULL.checkboolean());
        assertEquals("ERR message", luaError.get("err").checkjstring());
        assertEquals("hello", luaString1.checkjstring());
        assertEquals("", luaString2.checkjstring());
        assertEquals(1, luaNumber.checkint());

        assertEquals(4, luaArray.length());
        assertEquals("hello", luaArray.get(1).checkjstring());
        assertFalse(luaArray.get(2).checkboolean());
        assertEquals(2, luaArray.get(3).checkint());
        assertEquals("ab", luaArray.get(4).checkjstring());

        assertEquals(0, luaNullArray.length());

        assertEquals(2, luaNestArray.length());
        assertEquals(2, luaNestArray.get(1).checkint());
        assertEquals(2, luaNestArray.get(2).length());
        assertEquals("hello", luaNestArray.get(2).get(1).checkjstring());
        assertEquals(2, luaNestArray.get(2).get(2).checkint());

    }

}
