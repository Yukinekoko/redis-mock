package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import org.junit.Test;
import org.luaj.vm2.LuaValue;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.*;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class TestCommandParser {

    @Test
    public void testConsumeCharacter() throws ParseErrorException, EOFException {
        RedisProtocolParser parser = new RedisProtocolParser("a");
        assertEquals(parser.consumeByte(), 'a');
    }

    @Test
    public void testExpectCharacter() throws ParseErrorException, EOFException {
        RedisProtocolParser parser = new RedisProtocolParser("a");
        parser.expectByte((byte) 'a');
    }

    @Test
    public void testConsumeLong() throws ParseErrorException {
        RedisProtocolParser parser = new RedisProtocolParser("12345678901234\r");
        assertEquals(parser.consumePositiveLong(), 12345678901234L);
    }

    @Test
    public void testConsumeString() throws ParseErrorException {
        RedisProtocolParser parser = new RedisProtocolParser("abcd");
        assertEquals(parser.consumeSlice(4).toString(), "abcd");
    }

    @Test
    public void testConsumeCount1() throws ParseErrorException, EOFException {
        RedisProtocolParser parser = new RedisProtocolParser("*12\r\n");
        assertEquals(parser.consumeCount(), 12L);
    }

    @Test
    public void testConsumeCount2() throws EOFException {
        RedisProtocolParser parser = new RedisProtocolParser("*2\r");
        try {
            parser.consumeCount();
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }


    @Test
    public void testConsumeParameter() throws ParseErrorException {
        RedisProtocolParser parser = new RedisProtocolParser("$5\r\nabcde\r\n");
        assertEquals(parser.consumeParameter().toString(), "abcde");
    }

    @Test
    public void testParse() throws ParseErrorException, EOFException {
        RedisCommand cmd = RedisProtocolParser.parseCommand("*3\r\n$0\r\n\r\n$4\r\nabcd\r\n$2\r\nef\r\n");
        assertEquals(cmd.getParameters().get(0).toString(), "");
        assertEquals(cmd.getParameters().get(1).toString(), "abcd");
        assertEquals(cmd.getParameters().get(2).toString(), "ef");
    }

    @Test
    public void testConsumeCharacterError() throws ParseErrorException {
        RedisProtocolParser parser = new RedisProtocolParser("");
        try {
            parser.consumeByte();
            assertTrue(false);
        } catch (EOFException e) {
            // OK
        }
    }

    @Test
    public void testExpectCharacterError1() throws EOFException {
        RedisProtocolParser parser = new RedisProtocolParser("a");
        try {
            parser.expectByte((byte) 'b');
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testExpectCharacterError2() throws ParseErrorException {
        InputStream in = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException();
            }
        };
        RedisProtocolParser parser = new RedisProtocolParser(in);
        try {
            parser.expectByte((byte) 'b');
            assertTrue(false);
        } catch (EOFException e) {
            // OK
        }
    }

    @Test
    public void testConsumeLongError1() {
        RedisProtocolParser parser = new RedisProtocolParser("\r");
        try {
            parser.consumePositiveLong();
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testConsumeLongError2() {
        RedisProtocolParser parser = new RedisProtocolParser("100a");
        try {
            parser.consumePositiveLong();
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testConsumeLongError3() {
        RedisProtocolParser parser = new RedisProtocolParser("");
        try {
            parser.consumePositiveLong();
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testConsumeStringError() {
        RedisProtocolParser parser = new RedisProtocolParser("abc");
        try {
            parser.consumeSlice(4);
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testConsumeCountError1() throws EOFException {
        RedisProtocolParser parser = new RedisProtocolParser("$12\r\n");
        try {
            parser.consumeCount();
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testConsumeCountError2() throws EOFException {
        RedisProtocolParser parser = new RedisProtocolParser("*12\ra");
        try {
            parser.consumeCount();
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testConsumeParameterError1() {
        RedisProtocolParser parser = new RedisProtocolParser("$4\r\nabcde\r\n");
        try {
            parser.consumeParameter();
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testConsumeParameterError2() {
        RedisProtocolParser parser = new RedisProtocolParser("$4\r\nabc\r\n");
        try {
            parser.consumeParameter();
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testConsumeParameterError3() {
        RedisProtocolParser parser = new RedisProtocolParser("$4\r\nabc");
        try {
            parser.consumeParameter();
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testConsumeParameterError4() {
        RedisProtocolParser parser = new RedisProtocolParser("$4\r");
        try {
            parser.consumeParameter();
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testParseError() throws ParseErrorException, EOFException {
        try {
            RedisProtocolParser.parseCommand("*0\r\n");
            assertTrue(false);
        } catch (ParseErrorException e) {
            // OK
        }
    }

    @Test
    public void testParseRedis2Lua() throws EOFException, ParseErrorException {

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

        LuaValue luaOK =  RedisProtocolParser.parseRedis2Lua(new Slice(strOK));
        LuaValue luaPONG =  RedisProtocolParser.parseRedis2Lua(new Slice(strPONG));
        LuaValue luaNULL =  RedisProtocolParser.parseRedis2Lua(new Slice(strNULL));
        LuaValue luaError =  RedisProtocolParser.parseRedis2Lua(new Slice(strError));
        LuaValue luaString1 =  RedisProtocolParser.parseRedis2Lua(new Slice(strString1));
        LuaValue luaString2 =  RedisProtocolParser.parseRedis2Lua(new Slice(strString2));
        LuaValue luaNumber =  RedisProtocolParser.parseRedis2Lua(new Slice(strNumber));
        LuaValue luaArray =  RedisProtocolParser.parseRedis2Lua(new Slice(strArray));
        LuaValue luaNullArray =  RedisProtocolParser.parseRedis2Lua(new Slice(strNullArray));
        LuaValue luaNestArray =  RedisProtocolParser.parseRedis2Lua(new Slice(strNestArray));

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
