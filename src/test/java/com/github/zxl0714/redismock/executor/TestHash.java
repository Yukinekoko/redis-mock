package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public class TestHash extends TestCommandExecutor {

    @Test
    public void testHashSetAndGet() throws ParseErrorException, EOFException {
        assertCommandNull(array("hget", "set", "a"));
        assertCommandEquals(1, array("hset", "set", "a", "a"));
        assertCommandEquals("a", array("hget", "set", "a"));
        assertCommandEquals(1, array("hset", "set", "b", "b"));
        assertCommandEquals(1, array("hset", "set", "c", "c"));
        assertCommandEquals(0, array("hset", "set", "a", "aa"));
        assertCommandEquals("aa", array("hget", "set", "a"));
        assertCommandEquals("b", array("hget", "set", "b"));
        assertCommandEquals("c", array("hget", "set", "c"));

        // error
        assertCommandError(array("hset", "set", "a"));
        assertCommandError(array("hset", "set", "a", "a", "a"));
        assertCommandOK(array("set", "a", "a"));
        assertCommandError(array("hset", "a", "a", "a"));
        assertCommandError(array("hget", "a", "a"));
        assertCommandError(array("hget", "set"));
        assertCommandError(array("hget", "set", "a", "a"));

    }

    @Test
    public void testHmget() throws ParseErrorException, EOFException, IOException {
        assertCommandEquals(1, array("hset", "set", "b", "b"));
        assertCommandEquals(1, array("hset", "set", "c", "c"));
        assertCommandEquals(1, array("hset", "set", "a", "aa"));

        assertEquals(array("aa"), executor.execCommand(
            parse(array("hmget", "set", "a")), socket
        ).toString());
        assertEquals(array("aa", "b", "c"), executor.execCommand(
            parse(array("hmget", "set", "a", "b", "c")), socket
        ).toString());
        assertEquals("*1\r\n$-1\r\n", executor.execCommand(
            parse(array("hmget", "set", "aaa")), socket
        ).toString());
        assertEquals("*3\r\n$-1\r\n$2\r\naa\r\n$2\r\naa\r\n", executor.execCommand(
            parse(array("hmget", "set", "aaa", "a", "a")), socket
        ).toString());

        // error
        assertCommandOK(array("set", "a", "a"));
        assertCommandError(array("hmget", "a", "a"));
        assertCommandError(array("hmget", "set"));
    }

    @Test
    public void testHashDelAndExists() throws ParseErrorException, EOFException {
        assertCommandEquals(0, array("hexists", "set", "a"));
        assertCommandEquals(0, array("hdel", "set", "a"));

        assertCommandEquals(1, array("hset", "set", "a", "a"));
        assertCommandEquals(1, array("hset", "set", "b", "b"));
        assertCommandEquals(1, array("hset", "set", "c", "c"));
        assertCommandEquals(1, array("hexists", "set", "a"));
        assertCommandEquals(1, array("hexists", "set", "b"));
        assertCommandEquals(1, array("hexists", "set", "c"));

        assertCommandEquals(1, array("hdel", "set", "a", "a"));
        assertCommandEquals(2, array("hdel", "set", "a", "a", "b", "c"));

        assertCommandEquals(0, array("hexists", "set", "a"));
        assertCommandEquals(0, array("hexists", "set", "b"));
        assertCommandEquals(0, array("hexists", "set", "c"));

        // error
        assertCommandOK(array("set", "a", "a"));
        assertCommandError(array("hdel", "set"));
        assertCommandError(array("hdel", "a", "a"));
        assertCommandError(array("hexists", "a", "a"));
        assertCommandError(array("hexists", "set"));
        assertCommandError(array("hexists", "set", "a", "b"));
    }

    @Test
    public void testHgetall() throws ParseErrorException, EOFException, IOException {
        assertEquals(Response.EMPTY_LIST.toString(), executor.execCommand(
            parse(array("hgetall", "set")), socket
        ).toString());
        assertCommandEquals(1, array("hset", "set", "a", "1"));
        assertEquals(array("a", "1"), executor.execCommand(
            parse(array("hgetall", "set")), socket
        ).toString());
        assertCommandEquals(1, array("hset", "set", "b", "2"));
        assertEquals(array("a", "1", "b", "2"), executor.execCommand(
            parse(array("hgetall", "set")), socket
        ).toString());
        assertCommandEquals(1, array("hdel", "set", "a"));
        assertEquals(array("b", "2"), executor.execCommand(
            parse(array("hgetall", "set")), socket
        ).toString());

        // error
        assertCommandOK(array("set", "a", "a"));
        assertCommandError(array("hgetall", "a"));
        assertCommandError(array("hgetall"));
        assertCommandError(array("hgetall", "set", "b"));
    }

    @Test
    public void testHincrby() throws ParseErrorException, EOFException {
        assertCommandEquals(0, array("hincrby", "abc", "a", "0"));
        assertCommandEquals(2, array("hincrby", "abc", "a", "2"));
        assertCommandEquals(7, array("hincrby", "abc", "a", "5"));
        assertCommandEquals(1, array("hset", "set", "a", "1"));
        assertCommandEquals(-2, array("hincrby", "set", "a", "-3"));
        assertCommandEquals(-3, array("hincrby", "set", "b", "-3"));

        // error
        assertCommandOK(array("set", "a", "a"));
        assertCommandError(array("hincrby", "a", "a", "-3"));
        assertCommandError(array("hincrby", "set", "a"));
        assertCommandError(array("hincrby", "set", "a", "-3", "-a"));
        // 对字符串操作
        assertCommandEquals(0, array("hset", "set", "b", "b"));
        assertCommandError(array("hincrby", "set", "b", "-3"));
        // 传入字符串
        assertCommandError(array("hincrby", "set", "a", "abc"));
        // 输入范围超限
        assertCommandError(array("hincrby", "set", "s1", "9223372036854775808"));
        assertCommandError(array("hincrby", "set", "s2", "-9223372036854775809"));
        // 累加范围超限
        assertCommandEquals(9223372036854775807L, array("hincrby", "set", "s1", "9223372036854775807"));
        assertCommandEquals(-9223372036854775808L, array("hincrby", "set", "s2", "-9223372036854775808"));
        assertCommandError(array("hincrby", "set", "s1", "1"));
        assertCommandError(array("hincrby", "set", "s2", "-1"));

    }

    @Test
    public void testHincrbyfloat() throws ParseErrorException, EOFException {
        assertCommandEquals("1.21999999999999997", array("hincrbyfloat", "hash", "h1", "1.22"));
        assertCommandEquals("1.22000000012199994", array("hincrbyfloat", "hash", "h1", "1.22E-10"));
        assertCommandEquals("1219999999999999962334747426816", array("hincrbyfloat", "hash", "h1", "1.22E30"));
        assertCommandEquals("179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368",
            array("hincrbyfloat", "hash", "hh11", "1.7976931348623157E308"));

        //ERROR
        assertCommandOK(array("set", "s1", "s1"));
        assertCommandEquals(1, array("hset", "hash", "ha", "h1"));
        assertCommandError(array("hincrbyfloat", "hash", "ha", "1"));
        assertCommandError(array("hincrbyfloat", "hash", "h1", "a"));
        assertCommandError(array("hincrbyfloat", "s1", "s1", "1.22"));
        assertCommandError(array("hincrbyfloat", "hash", "h1"));
        assertCommandError(array("hincrbyfloat", "hash", "h1", "1", "1"));
        assertCommandError(array("hincrbyfloat", "hash", "h2", "1.7976931348623157E309"));
        assertCommandError(array("hincrbyfloat", "hash", "hh11", "1.7976931348623157E308"));
    }

    @Test
    public void testHkeys() throws ParseErrorException, EOFException, IOException {
        assertEquals(Response.EMPTY_LIST.toString(), executor.execCommand(
            parse(array("hkeys", "set")), socket
        ).toString());
        assertCommandEquals(1, array("hset", "set", "a", "1"));
        assertEquals(array("a"), executor.execCommand(
            parse(array("hkeys", "set")), socket
        ).toString());
        assertCommandEquals(1, array("hset", "set", "b", "2"));
        assertEquals(array("a", "b"), executor.execCommand(
            parse(array("hkeys", "set")), socket
        ).toString());

        //error
        assertCommandOK(array("set", "a", "a"));
        assertCommandError(array("hkeys", "a"));
        assertCommandError(array("hkeys"));
        assertCommandError(array("hkeys", "set", "a"));
    }

    @Test
    public void testHlen() throws ParseErrorException, EOFException {
        assertCommandEquals(0, array("hlen", "set"));
        assertCommandEquals(1, array("hset", "set", "a", "1"));
        assertCommandEquals(1, array("hlen", "set"));
        assertCommandEquals(1, array("hset", "set", "b", "2"));
        assertCommandEquals(1, array("hset", "set", "c", "3"));
        assertCommandEquals(3, array("hlen", "set"));

        // error
        assertCommandOK(array("set", "a", "a"));
        assertCommandError(array("hlen", "a"));
        assertCommandError(array("hlen"));
        assertCommandError(array("hlen", "set", "a"));
    }

    @Test
    public void testHmset() throws ParseErrorException, EOFException {
        assertCommandOK(array("hmset", "set", "a", "1", "b", "2"));
        assertCommandEquals("1", array("hget", "set", "a"));
        assertCommandEquals("2", array("hget", "set", "b"));
        assertCommandOK(array("hmset", "set", "a", "a", "b", "b"));
        assertCommandEquals("a", array("hget", "set", "a"));
        assertCommandOK(array("hmset", "set", "a", "1", "a", "2"));
        assertCommandEquals("2", array("hget", "set", "a"));

        // error
        assertCommandError(array("hmset", "set", "a"));
        assertCommandError(array("hmset", "set", "a", "1", "b"));
        assertCommandError(array("hmset", "set"));
        assertCommandOK(array("set", "a", "a"));
        assertCommandError(array("hmset", "a", "a", "1"));
    }

    @Test
    public void testHsetnx() throws ParseErrorException, EOFException {
        assertCommandEquals(1, array("hsetnx", "h1", "key", "val"));
        assertCommandEquals("val", array("hget", "h1", "key"));
        assertCommandEquals(0, array("hsetnx", "h1", "key", "newval"));
        assertCommandEquals("val", array("hget", "h1", "key"));

        // error
        assertCommandOK(array("set", "s1", "str"));
        assertCommandError(array("hsetnx", "s1", "key", "val"));
        assertCommandError(array("hsetnx", "h1", "key"));
        assertCommandError(array("hsetnx", "h1", "key", "val", "val"));
    }

    @Test
    public void testHstrlen() throws ParseErrorException, EOFException {
        assertCommandEquals(0, array("hstrlen", "h1", "key"));
        assertCommandEquals(1, array("hsetnx", "h1", "key", "val"));
        assertCommandEquals(3, array("hstrlen", "h1", "key"));
        assertCommandEquals(0, array("hset", "h1", "key", "helloworld"));
        assertCommandEquals(10, array("hstrlen", "h1", "key"));
        assertCommandEquals(1, array("hset", "h1", "key1", "-10"));
        assertCommandEquals(3, array("hstrlen", "h1", "key1"));
        assertCommandEquals(0, array("hstrlen", "h1", "keynull"));

        // error
        assertCommandOK(array("set", "s1", "str"));
        assertCommandError(array("hstrlen", "s1", "key"));
        assertCommandError(array("hstrlen", "h1", "key", "key"));
        assertCommandError(array("hstrlen", "h1"));

    }

    @Test
    public void testHvals() throws ParseErrorException, EOFException, IOException {
        assertEquals(Response.EMPTY_LIST.toString(), executor.execCommand(
            parse(array("hvals", "h1")), socket
        ).toString());
        assertCommandOK(array("hmset", "h1", "k1", "v1", "k2", "v2"));
        assertEquals(array("v1", "v2"), executor.execCommand(
            parse(array("hvals", "h1")), socket
        ).toString());

        // error
        assertCommandOK(array("set", "s1", "str"));
        assertCommandError(array("hvals", "s1"));
        assertCommandError(array("hvals", "h1", "h1"));
        assertCommandError(array("hvals"));
    }

    @Test
    public void testHscan() throws ParseErrorException, EOFException, IOException {
        // normal
        assertEquals("*2\r\n$1\r\n0\r\n*-1\r\n",
            exec(array("hscan", "h1", "0")));
        assertCommandOK(array("hmset", "h1",
            "k1", "v1",
            "k2", "v2",
            "k3", "v3",
            "k4", "v4",
            "k5", "v5",
            "k6", "v6",
            "k7", "v7",
            "k8", "v8",
            "k9", "v9",
            "k10", "v10"));
        assertEquals("*2\r\n$1\r\n0\r\n*20\r\n" +
                "$2\r\nk1\r\n$2\r\nv1\r\n" +
                "$2\r\nk2\r\n$2\r\nv2\r\n" +
                "$2\r\nk3\r\n$2\r\nv3\r\n" +
                "$2\r\nk4\r\n$2\r\nv4\r\n" +
                "$2\r\nk5\r\n$2\r\nv5\r\n" +
                "$3\r\nk10\r\n$3\r\nv10\r\n" +
                "$2\r\nk6\r\n$2\r\nv6\r\n" +
                "$2\r\nk7\r\n$2\r\nv7\r\n" +
                "$2\r\nk8\r\n$2\r\nv8\r\n" +
                "$2\r\nk9\r\n$2\r\nv9\r\n",
            exec(array("hscan", "h1", "0")));
        // count
        assertEquals("*2\r\n$1\r\n5\r\n*10\r\n" +
                "$2\r\nk1\r\n$2\r\nv1\r\n" +
                "$2\r\nk2\r\n$2\r\nv2\r\n" +
                "$2\r\nk3\r\n$2\r\nv3\r\n" +
                "$2\r\nk4\r\n$2\r\nv4\r\n" +
                "$2\r\nk5\r\n$2\r\nv5\r\n",
            exec(array("hscan", "h1", "0", "count", "5")));
        assertEquals("*2\r\n$1\r\n0\r\n*10\r\n" +
                "$3\r\nk10\r\n$3\r\nv10\r\n" +
                "$2\r\nk6\r\n$2\r\nv6\r\n" +
                "$2\r\nk7\r\n$2\r\nv7\r\n" +
                "$2\r\nk8\r\n$2\r\nv8\r\n" +
                "$2\r\nk9\r\n$2\r\nv9\r\n",
            exec(array("hscan", "h1", "5", "count", "5")));
        // match
        assertEquals("*2\r\n$1\r\n5\r\n*2\r\n" +
                "$2\r\nk1\r\n$2\r\nv1\r\n",
            exec(array("hscan", "h1", "0", "count", "5", "match", "k1")));
        assertEquals("*2\r\n$1\r\n0\r\n*4\r\n" +
                "$2\r\nk1\r\n$2\r\nv1\r\n" +
                "$3\r\nk10\r\n$3\r\nv10\r\n",
            exec(array("hscan", "h1", "0", "count", "10", "match", "k1*")));
        // error
        assertCommandOK(array("set", "s1", "str"));
        assertCommandError(array("hscan", "s1", "0"));
        assertCommandError(array("hscan", "h1"));
        assertCommandError(array("hscan", "h1", "-1"));
        assertCommandError(array("hscan", "h1", "abc"));
        assertCommandError(array("hscan", "h1", "0", "count", "0"));
        assertCommandError(array("hscan", "h1", "0", "count", "-1"));
        assertCommandError(array("hscan", "h1", "0", "count", "abc"));
        assertCommandError(array("hscan", "h1", "0", "count"));
        assertCommandError(array("hscan", "h1", "0", "baba", "0"));
        assertCommandError(array("hscan", "h1", "0", "count", "1", "match"));
        assertCommandError(array("hscan", "h1", "0", "count", "1", "match", "*", "a"));
    }
}
