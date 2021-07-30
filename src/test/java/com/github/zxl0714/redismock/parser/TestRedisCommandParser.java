package com.github.zxl0714.redismock.parser;

import com.github.zxl0714.redismock.RedisCommand;
import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.github.zxl0714.redismock.parser.LuaToRedisReplyParser;
import com.github.zxl0714.redismock.parser.RedisCommandParser;
import com.github.zxl0714.redismock.parser.RedisToLuaReplyParser;
import org.junit.Before;
import org.junit.Test;
import org.luaj.vm2.LuaValue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class TestRedisCommandParser {

    private RedisCommandParser parse;

    private InputStream getInputStream(String command) {
        return new ByteArrayInputStream(command.getBytes());
    }

    private void setMessage(String message) throws NoSuchFieldException, IllegalAccessException {
        Field field = RedisCommandParser.class.getSuperclass().getDeclaredField("input");
        field.setAccessible(true);
        field.set(parse, getInputStream(message));

    }

    @Before
    public void init() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Constructor<? extends RedisCommandParser> constructor =
            RedisCommandParser.class.getDeclaredConstructor(InputStream.class);
        constructor.setAccessible(true);
        parse = constructor.newInstance(new ByteArrayInputStream("".getBytes()));


    }

    @Test
    public void testConsumeCharacter() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        setMessage("a");
        Method method = RedisCommandParser.class.getSuperclass().getDeclaredMethod("consumeByte");
        method.setAccessible(true);
        assertEquals((byte)method.invoke(parse), 'a');
    }

    @Test
    public void testExpectCharacter() throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        setMessage("a");
        Method method = RedisCommandParser.class.getSuperclass().getDeclaredMethod("expectByte", byte.class);
        method.setAccessible(true);
        method.invoke(parse, (byte)'a');
    }

    @Test
    public void testConsumeLong() throws Exception {
        setMessage("12345678901234\r");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumePositiveLong");
        method.setAccessible(true);
        assertEquals((long)method.invoke(parse), 12345678901234L);
    }

    @Test
    public void testConsumeString() throws Exception {
        setMessage("abcd");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeSlice", long.class);
        method.setAccessible(true);
        assertEquals( method.invoke(parse, 4L).toString(), "abcd");
    }

    @Test
    public void testConsumeCount1() throws Exception {
        setMessage("*12\r\n");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeCount");
        method.setAccessible(true);
        assertEquals(method.invoke(parse), 12L);
    }

    @Test
    public void testConsumeCount2() throws Exception {
        setMessage("*2\r");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeCount");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }


    @Test
    public void testConsumeParameter() throws Exception {
        setMessage("$5\r\nabcde\r\n");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeParameter");
        method.setAccessible(true);
        assertEquals(method.invoke(parse).toString(), "abcde");
    }

    @Test
    public void testParse() throws Exception {
        RedisCommand cmd = RedisCommandParser.parse(getInputStream("*3\r\n$0\r\n\r\n$4\r\nabcd\r\n$2\r\nef\r\n"));
        assertEquals(cmd.getParameters().get(0).toString(), "");
        assertEquals(cmd.getParameters().get(1).toString(), "abcd");
        assertEquals(cmd.getParameters().get(2).toString(), "ef");
    }

    @Test
    public void testConsumeCharacterError() throws Exception {
        setMessage("");
        Method method = RedisCommandParser.class.getSuperclass().getDeclaredMethod("consumeByte");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testExpectCharacterError1() throws Exception {
        setMessage("a");
        Method method = RedisCommandParser.class.getSuperclass().getDeclaredMethod("expectByte", byte.class);
        method.setAccessible(true);
        try {
            method.invoke(parse, (byte)'b');
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testExpectCharacterError2() throws Exception {
        InputStream in = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException();
            }
        };
        RedisCommandParser parser = new RedisCommandParser(in);
        Method method = RedisCommandParser.class.getSuperclass().getDeclaredMethod("expectByte", byte.class);
        try {
            method.invoke(parser, (byte)'b');
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testConsumeLongError1() throws Exception {
        setMessage("\r");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumePositiveLong");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testConsumeLongError2() throws Exception {
        setMessage("100a");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumePositiveLong");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testConsumeLongError3() throws Exception {
        setMessage("");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumePositiveLong");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testConsumeStringError() throws Exception {
        setMessage("abc");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeSlice", long.class);
        method.setAccessible(true);
        try {
            method.invoke(parse, 4L);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testConsumeCountError1() throws Exception {
        setMessage("$12\r\n");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeCount");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testConsumeCountError2() throws Exception {
        setMessage("*12\ra");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeCount");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testConsumeParameterError1() throws Exception{
        setMessage("$4\r\nabcde\r\n");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeParameter");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testConsumeParameterError2() throws Exception {
        setMessage("$4\r\nabc\r\n");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeParameter");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testConsumeParameterError3() throws Exception {
        setMessage("$4\r\nabc");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeParameter");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testConsumeParameterError4() throws Exception {
        setMessage("$4\r");
        Method method = RedisCommandParser.class.getDeclaredMethod("consumeParameter");
        method.setAccessible(true);
        try {
            method.invoke(parse);
            throw new AssertionError();
        } catch (Exception e) {
            // OK
        }
    }

    @Test
    public void testParseError() throws EOFException {
        try {
            RedisCommandParser.parse(getInputStream("*0\r\n"));
            throw new AssertionError();
        } catch (ParseErrorException e) {
            // OK
        }
    }




}
