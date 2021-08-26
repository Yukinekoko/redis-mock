package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.github.zxl0714.redismock.expecptions.UnsupportedScriptCommandException;
import com.github.zxl0714.redismock.parser.RedisCommandParser;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class TestCommandExecutor {

    private static final String CRLF = "\r\n";
    
    private static CommandExecutor executor;
    
    private Socket socket;
    
    private ByteArrayOutputStream outputStream;

    private static String bulkString(CharSequence param) {
        return "$" + param.length() + CRLF + param.toString() + CRLF;
    }

    private static String bulkLong(Long param) {
        return ":" + param + CRLF;
    }

    private static String array(CharSequence ...params) {
        StringBuilder builder = new StringBuilder();
        builder.append('*').append(params.length).append(CRLF);
        for (CharSequence param : params) {
            if (param == null) {
                builder.append("$-1").append(CRLF);
            } else {
                builder.append(bulkString(param));
            }
        }
        return builder.toString();
    }

    /**
     * 构造响应的列表
     * */
    private static String responseArray(Object ...params) {
        StringBuilder builder = new StringBuilder();
        builder.append('*').append(params.length).append(CRLF);
        for (Object param : params) {
            if (param == null) {
                builder.append("$-1").append(CRLF);
            } else if (param instanceof CharSequence) {
                builder.append(bulkString((CharSequence) param));
            } else if (param instanceof Long) {
                builder.append(bulkLong((Long) param));
            } else if (param instanceof Slice) {
                builder.append(param.toString());
            }
        }
        return builder.toString();
    }

    private RedisCommand parse(String command) throws ParseErrorException, EOFException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(command.getBytes());
        return RedisCommandParser.parse(inputStream);
    }

    private String exec(String command) throws ParseErrorException, EOFException, IOException {
        return executor.execCommand(parse(command), socket).toString();
    }

    private void assertCommandEquals(String expect, String command) throws ParseErrorException, EOFException {
        try {
            assertEquals(bulkString(expect), executor.execCommand(parse(command), socket).toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assertCommandArrayEquals(String array, String command) throws ParseErrorException, EOFException {
        try {
            assertEquals(array, executor.execCommand(parse(command), socket).toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assertCommandEquals(long expect, String command) throws ParseErrorException, EOFException {
        try {
            assertEquals(Response.integer(expect), executor.execCommand(parse(command), socket));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assertCommandNull(String command) throws ParseErrorException, EOFException {
        try {
            assertEquals(Response.NULL, executor.execCommand(parse(command), socket));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assertCommandOK(String command) throws ParseErrorException, EOFException {
        try {
            assertEquals(Response.OK, executor.execCommand(parse(command), socket));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assertCommandError(String command) throws ParseErrorException, EOFException {
        try {
            assertEquals('-', executor.execCommand(parse(command), socket).data()[0]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void initCommandExecutor() throws IOException {
        executor = new CommandExecutor(new RedisBase());
        Socket socket = mock(Socket.class);
        outputStream = new ByteArrayOutputStream();
        when(socket.getOutputStream()).thenReturn(outputStream);

        SocketAttributes socketAttributes = new SocketAttributes();
        socketAttributes.setDatabaseIndex(0);
        socketAttributes.setSocket(socket);
        socketAttributes.setCommandExecutor(executor);
        SocketContextHolder.setSocketAttributes(socketAttributes);

    }

    @Test
    public void testSetAndGet() throws ParseErrorException, EOFException {
        assertCommandNull(array("GET", "ab"));
        assertCommandOK(array("SET", "ab", "abc"));
        assertCommandEquals("abc", array("GET", "ab"));
        assertCommandOK(array("SET", "ab", "abd"));
        assertCommandEquals("abd", array("GET", "ab"));
        assertCommandNull(array("GET", "ac"));
    }

    @Test
    public void testWrongNumberOfArguments() throws ParseErrorException, EOFException {
        assertCommandError(array("SET", "ab"));
        assertCommandError(array("pfcount"));
    }

    @Test
    public void testUnknownCommand() throws ParseErrorException, EOFException {
        assertCommandError(array("unknown"));
    }

    @Test
    public void testExpire() throws ParseErrorException, InterruptedException, EOFException {
        assertCommandEquals(0, array("expire", "ab", "1"));
        assertCommandOK(array("SET", "ab", "abd"));
        assertCommandEquals(1, array("expire", "ab", "1"));
        assertCommandEquals("abd", array("GET", "ab"));
        assertCommandError(array("expire", "ab", "a"));
        Thread.sleep(1000);
        assertCommandNull(array("GET", "ab"));
    }

    @Test
    public void testTTL() throws ParseErrorException, InterruptedException, EOFException {
        assertCommandEquals(-2, array("ttl", "ab"));
        assertCommandOK(array("SET", "ab", "abd"));
        assertCommandEquals(-1, array("ttl", "ab"));
        assertCommandEquals(1, array("expire", "ab", "2"));
        assertCommandEquals(2, array("ttl", "ab"));
        Thread.sleep(1000);
        assertCommandEquals(1, array("ttl", "ab"));
        Thread.sleep(1000);
        assertCommandEquals(-2, array("ttl", "ab"));
    }

    @Test
    public void testPTTL() throws ParseErrorException, InterruptedException, EOFException, IOException {
        assertCommandEquals(-2, array("pttl", "ab"));
        assertCommandOK(array("SET", "ab", "abd"));
        assertCommandEquals(-1, array("pttl", "ab"));
        assertCommandEquals(1, array("expire", "ab", "2"));
        assertTrue(executor.execCommand(parse(array("pttl", "ab")), socket).compareTo(Response.integer(1900L)) > 0);
        Thread.sleep(1100);
        assertTrue(executor.execCommand(parse(array("pttl", "ab")), socket).compareTo(Response.integer(999L)) < 0);
        Thread.sleep(1000);
        assertCommandEquals(-2, array("pttl", "ab"));
    }

    @Test
    public void testIncr() throws ParseErrorException, EOFException {
        assertCommandEquals(1, array("incr", "a"));
        assertCommandEquals(2, array("incr", "a"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("incr", "a"));
    }

    @Test
    public void testIncrBy() throws ParseErrorException, EOFException {
        assertCommandEquals(5, array("incrby", "a", "5"));
        assertCommandEquals(11, array("incrby", "a", "6"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("incrby", "a", "1"));
    }

    @Test
    public void testDecr() throws ParseErrorException, EOFException {
        assertCommandEquals(-1, array("decr", "a"));
        assertCommandEquals(-2, array("decr", "a"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("decr", "a"));
    }

    @Test
    public void testDecrBy() throws ParseErrorException, EOFException {
        assertCommandEquals(-5, array("decrby", "a", "5"));
        assertCommandEquals(-11, array("decrby", "a", "6"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("decrby", "a", "1"));
    }

    @Test
    public void testHll() throws ParseErrorException, EOFException {
        assertCommandEquals(1, array("pfadd", "a", "b", "c"));
        assertCommandEquals(0, array("pfadd", "a", "b", "c"));
        assertCommandEquals(2, array("pfcount", "a"));
        assertCommandEquals(0, array("pfcount", "b"));
        assertCommandEquals(1, array("pfadd", "b", "c", "d"));
        assertCommandEquals(3, array("pfcount", "a", "b"));
        assertCommandOK(array("pfmerge", "c"));
        assertCommandEquals(0, array("pfcount", "c"));
        assertCommandOK(array("pfmerge", "a", "b"));
        assertCommandEquals(3, array("pfcount", "a"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("pfcount", "a"));
        assertCommandError(array("pfmerge", "a"));
        assertCommandError(array("pfmerge", "b", "a"));
        assertCommandError(array("pfadd", "a", "b"));
    }

    @Test
    public void testAppend() throws ParseErrorException, EOFException {
        assertCommandEquals(3, array("append", "ab", "abc"));
        assertCommandEquals(6, array("append", "ab", "abc"));
        assertCommandEquals("abcabc", array("GET", "ab"));
    }

    @Test
    public void testSetAndGetBit() throws ParseErrorException, EOFException, IOException {
        assertCommandEquals(0, array("getbit", "mykey", "7"));
        assertCommandEquals(0, array("setbit", "mykey", "7", "1"));
        assertCommandEquals(1, array("getbit", "mykey", "7"));
        assertCommandEquals(0, array("getbit", "mykey", "6"));
        assertCommandEquals(1, array("setbit", "mykey", "7", "0"));
        assertEquals(Response.bulkString(new Slice(new byte[]{0})),
                executor.execCommand(parse(array("get", "mykey")), socket));
        assertCommandEquals(0, array("setbit", "mykey", "33", "1"));
        assertEquals(Response.bulkString(new Slice(new byte[]{0, 0, 0, 0, 2})),
                executor.execCommand(parse(array("get", "mykey")), socket));
        assertCommandEquals(0, array("setbit", "mykey", "22", "1"));
        assertCommandEquals(0, array("getbit", "mykey", "117"));
        assertCommandError(array("getbit", "mykey", "a"));
        assertCommandError(array("getbit", "mykey"));
        assertCommandError(array("setbit", "mykey", "a", "1"));
        assertCommandError(array("setbit", "mykey", "1"));
        assertCommandError(array("setbit", "mykey", "1", "a"));
        assertCommandError(array("setbit", "mykey", "1", "2"));
    }

    @Test
    public void testSetex() throws ParseErrorException, EOFException {
        assertCommandOK(array("SETex", "ab", "100", "k"));
        assertCommandEquals(100, array("ttl", "ab"));
        assertCommandError(array("SETex", "ab", "10a", "k"));
    }

    @Test
    public void testPsetex() throws ParseErrorException, EOFException, IOException {
        assertCommandOK(array("pSETex", "ab", "99", "k"));
        assertTrue(executor.execCommand(parse(array("pttl", "ab")), socket).compareTo(Response.integer(90)) > 0);
        assertTrue(executor.execCommand(parse(array("pttl", "ab")), socket).compareTo(Response.integer(99)) <= 0);
        assertCommandError(array("pSETex", "ab", "10a", "k"));
    }

    @Test
    public void testSetnx() throws ParseErrorException, EOFException {
        assertCommandEquals(1, array("setnx", "k", "vvv"));
        assertCommandEquals("vvv", array("get", "k"));
        assertCommandEquals(0, array("setnx", "k", "ggg"));
        assertCommandEquals("vvv", array("get", "k"));
    }

    @Test
    public void testMset() throws ParseErrorException, EOFException {
        assertCommandOK(array("mset", "k1", "a", "k2", "b"));
        assertCommandEquals("a", array("GET", "k1"));
        assertCommandEquals("b", array("GET", "k2"));
        assertCommandError(array("mset", "k1", "a", "k2"));
    }

    @Test
    public void testMget() throws ParseErrorException, EOFException, IOException {
        assertCommandOK(array("SET", "a", "abc"));
        assertCommandOK(array("SET", "b", "abd"));

        assertEquals(array("abc", "abd", null),
                executor.execCommand(parse(array("mget", "a", "b", "c")), socket).toString());
    }

    @Test
    public void testGetset() throws ParseErrorException, EOFException {
        assertCommandNull(array("getSET", "a", "abc"));
        assertCommandEquals("abc", array("getSET", "a", "abd"));
    }

    @Test
    public void testStrlen() throws ParseErrorException, EOFException {
        assertCommandEquals(0, array("strlen", "a"));
        assertCommandOK(array("SET", "a", "abd"));
        assertCommandEquals(3, array("strlen", "a"));
    }

    @Test
    public void testDel() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "a", "v"));
        assertCommandOK(array("set", "b", "v"));
        assertCommandEquals(2, array("del", "a", "b", "c"));
        assertCommandNull(array("get", "a"));
        assertCommandNull(array("get", "b"));
    }

    @Test
    public void testExists() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "a", "v"));
        assertCommandEquals(1, array("exists", "a"));
        assertCommandEquals(0, array("exists", "b"));
    }

    @Test
    public void testExpireAt() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "a", "v"));
        assertCommandEquals(1, array("expireat", "a", "1293840000"));
        assertCommandEquals(0, array("exists", "a"));
        assertCommandOK(array("set", "a", "v"));
        long now = System.currentTimeMillis() / 1000 + 5;
        assertCommandEquals(1, array("expireat", "a", String.valueOf(now)));
        assertCommandEquals(5, array("ttl", "a"));
        assertCommandError(array("expireat", "a", "a"));
    }

    @Test
    public void testPexpireAt() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "a", "v"));
        assertCommandEquals(1, array("pexpireat", "a", "1293840000000"));
        assertCommandEquals(0, array("exists", "a"));
        assertCommandEquals(0, array("pexpireat", "a", "1293840000000"));
        assertCommandOK(array("set", "a", "v"));
        long now = System.currentTimeMillis() + 5000;
        assertCommandEquals(1, array("pexpireat", "a", String.valueOf(now)));
        assertCommandEquals(5, array("ttl", "a"));
        assertCommandError(array("pexpireat", "a", "a"));
    }

    @Test
    public void testPexpire() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "a", "v"));
        assertCommandEquals(1, array("pexpire", "a", "1500000"));
        assertCommandEquals(1500, array("ttl", "a"));
    }

    @Test
    public void testLpush() throws ParseErrorException, EOFException, IOException {
        assertCommandEquals(1, array("lpush", "mylist", "!"));
        assertCommandEquals(3, array("lpush", "mylist", "world", "hello"));
        assertEquals(array("hello", "world", "!"),
                executor.execCommand(parse(array("lrange", "mylist", "0", "-1")), socket).toString());
        assertCommandOK(array("set", "a", "v"));
        assertCommandError(array("lpush", "a", "1"));
    }

    @Test
    public void testLrange() throws ParseErrorException, EOFException, IOException {
        assertEquals(array(),
                executor.execCommand(parse(array("lrange", "mylist", "0", "-1")), socket).toString());
        assertCommandEquals(3, array("lpush", "mylist", "1", "2", "3"));
        assertEquals(array("3", "2", "1"),
                executor.execCommand(parse(array("lrange", "mylist", "0", "-1")), socket).toString());
        assertEquals(array("3", "2", "1"),
                executor.execCommand(parse(array("lrange", "mylist", "-10", "10")), socket).toString());
        assertEquals(array("2"),
                executor.execCommand(parse(array("lrange", "mylist", "1", "-2")), socket).toString());
        assertEquals(array(),
                executor.execCommand(parse(array("lrange", "mylist", "10", "-10")), socket).toString());
        assertCommandError(array("lrange", "mylist", "a", "-1"));
        assertCommandOK(array("set", "a", "v"));
        assertCommandError(array("lrange", "a", "0", "-1"));
    }

    @Test
    public void testLlen() throws ParseErrorException, EOFException {
        assertCommandEquals(0, array("llen", "a"));
        assertCommandEquals(3, array("lpush", "mylist", "3", "2", "1"));
        assertCommandEquals(3, array("llen", "mylist"));
        assertCommandOK(array("set", "a", "v"));
        assertCommandError(array("llen", "a"));
    }

    @Test
    public void testLpushx() throws ParseErrorException, EOFException, IOException {
        assertCommandEquals(1, array("lpush", "a", "1"));
        assertCommandEquals(2, array("lpushx", "a", "2"));
        assertEquals(array("2", "1"),
                executor.execCommand(parse(array("lrange", "a", "0", "-1")), socket).toString());
        assertCommandEquals(0, array("lpushx", "b", "1"));
        assertCommandOK(array("set", "a", "v"));
        assertCommandError(array("lpushx", "a", "1"));
    }

    @Test
    public void testLpop() throws ParseErrorException, EOFException {
        assertCommandEquals(2, array("lpush", "list", "2", "1"));
        assertCommandEquals("1", array("lpop", "list"));
        assertCommandEquals("2", array("lpop", "list"));
        assertCommandNull(array("lpop", "list"));
        assertCommandNull(array("lpop", "notexist"));
        assertCommandOK(array("set", "key", "value"));
        assertCommandError(array("lpop", "key"));
    }

    @Test
    public void testLindex() throws ParseErrorException, EOFException {
        assertCommandEquals(2, array("lpush", "list", "1", "2"));
        assertCommandEquals("2", array("lindex", "list", "0"));
        assertCommandEquals("1", array("lindex", "list", "-1"));
        assertCommandNull(array("lindex", "list", "2"));
        assertCommandNull(array("lindex", "list", "-3"));
        assertCommandError(array("lindex", "list", "a"));
        assertCommandNull(array("lindex", "notexist", "0"));
        assertCommandOK(array("set", "key", "value"));
        assertCommandError(array("lindex", "key", "1"));
    }

    @Test
    public void testRpush() throws ParseErrorException, EOFException, IOException {
        assertCommandEquals(1, array("rpush", "mylist", "!"));
        assertCommandEquals(3, array("rpush", "mylist", "world", "hello"));
        assertEquals(array("!", "world", "hello"),
                executor.execCommand(parse(array("lrange", "mylist", "0", "-1")), socket).toString());
        assertCommandOK(array("set", "a", "v"));
        assertCommandError(array("rpush", "a", "1"));
    }

    @Test
    public void testKeys() throws ParseErrorException, EOFException, IOException {
        assertCommandOK(array("set", "prefix:a", "a"));
        assertCommandOK(array("set", "prefix:b", "b"));
        assertEquals(array("prefix:a","prefix:b"),
                executor.execCommand(parse(array("keys", "prefix:*")), socket).toString());
    }

    @Test
    public void testSelect() throws ParseErrorException, EOFException {
        assertCommandNull(array("get", "ab"));
        assertCommandOK(array("SET", "ab", "abc"));
        assertCommandEquals("abc", array("GET", "ab"));
        assertCommandOK(array("select", "3"));
        assertCommandNull(array("get", "ab"));
        assertCommandOK(array("SET", "ab", "base3"));
        assertCommandEquals("base3", array("GET", "ab"));
        assertCommandOK(array("select", "0"));
        assertCommandEquals("abc", array("GET", "ab"));
        // error
        assertCommandError(array("select", "20"));
        assertCommandError(array("select", "-10"));
        assertCommandError(array("select", "abc"));
    }

    @Test
    public void testPublish() throws ParseErrorException, EOFException {
        assertCommandEquals(0,
                array("publish", "channel_1", "Hello World"));
    }

    @Test
    public void testSubscribe() throws ParseErrorException, EOFException, IOException {
        assertEquals(responseArray("subscribe", "channel_1", 1L),
                executor.execCommand(parse(array("subscribe", "channel_1")), socket).toString());
        assertEquals(responseArray("subscribe", "channel_1", 1L),
                executor.execCommand(parse(array("subscribe", "channel_1")), socket).toString());
        assertEquals(responseArray("subscribe", "channel_2", 1L, "subscribe", "channel_3", 2L),
                executor.execCommand(parse(array("subscribe", "channel_2", "channel_3")), socket).toString());
    }

    @Test
    public void testUnsubscribe() throws ParseErrorException, EOFException, IOException {
        assertEquals(responseArray("unsubscribe", Response.NULL, 0L),
                executor.execCommand(parse(array("unsubscribe", "channel_1")), socket).toString());
        assertEquals(responseArray("unsubscribe", Response.NULL, 0L),
                executor.execCommand(
                        parse(array("unsubscribe", "channel_1", "c2", "c3")), socket).toString());
    }

    @Test
    public void testEval() throws ParseErrorException, EOFException, IOException {
        assertEquals(Response.PONG,
            executor.execCommand(parse(array("ping", "return redis.call('ping')", "0")), socket));

        assertCommandEquals("abc", array("eval", "redis.pcall('set'); return 'abc'", "0"));
        assertCommandError(array("eval", "redis.call('set'); return 'abc'", "0"));
        assertCommandError(array("eval", "redis.call('quit'); return 'abc'", "0"));


    }

    @Test
    public void testHashSetAndGet() throws ParseErrorException, EOFException, IOException {
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

    @Test
    public void testSet() throws ParseErrorException, EOFException, IOException {
        // SADD SCARD SMEMBERS
        assertCommandEquals(0, array("scard", "set1"));
        assertEquals(array(), exec(array("smembers", "set1")));
        assertCommandEquals(1, array("sadd", "set1", "k1"));
        assertEquals(array("k1"), exec(array("smembers", "set1")));
        assertCommandEquals(1, array("scard", "set1"));
        assertCommandEquals(0, array("sadd", "set1", "k1"));
        assertCommandEquals(1, array("scard", "set1"));
        assertCommandEquals(3, array("sadd", "set1", "k2", "k3", "k4"));
        assertEquals(array("k2", "k3", "k4", "k1"), exec(array("smembers", "set1")));
        assertCommandEquals(2, array("sadd", "set1", "k4", "k5", "k6"));
        assertCommandEquals(6, array("scard", "set1"));
        // SDIFF
        assertCommandEquals(3, array("sadd", "set2", "k2", "k3", "k4"));
        assertCommandEquals(3, array("sadd", "set3", "k4", "k5", "k7"));
        assertEquals(array("k5", "k6", "k1"), exec(array("sdiff", "set1", "set2")));
        assertEquals(array("k2", "k3", "k6", "k1"), exec(array("sdiff", "set1", "set3")));
        assertEquals(array("k6", "k1"), exec(array("sdiff", "set1", "set2", "set3")));
        assertEquals(array(), exec(array("sdiff", "set2", "set1")));
        assertEquals(array("k2", "k3", "k4"), exec(array("sdiff", "set2")));
        assertEquals(array("k2", "k3", "k4"), exec(array("sdiff", "set2", "set0")));
        // SDIFFSTORE
        assertCommandEquals(3, array("sdiffstore", "set4", "set2"));
        assertEquals(array("k2", "k3", "k4"), exec(array("smembers", "set4")));
        assertCommandEquals(3, array("sdiffstore", "set4", "set1", "set2"));
        assertEquals(array("k6", "k5", "k1"), exec(array("smembers", "set4")));
        assertCommandEquals(0, array("sdiffstore", "set4", "set2", "set1"));
        assertEquals(array(), exec(array("smembers", "set4")));
        assertCommandOK(array("set", "str1", "str1"));
        assertCommandEquals(3, array("sdiffstore", "str1", "set1", "set2"));
        assertEquals(array("k6", "k5", "k1"), exec(array("smembers", "str1")));
        /*
        * SINTER
        * set1: k1 k2 k3 k4 k5 k6
        * set2: k2 k3 k4
        * set3: k4 k5 k7
        * */
        assertEquals(array("k4", "k5"), exec(array("sinter", "set1", "set3")));
        assertEquals(array("k4"), exec(array("sinter", "set1", "set2", "set3")));
        assertEquals(array("k2", "k3", "k4"), exec(array("sinter", "set2")));
        assertEquals(array(), exec(array("sinter", "set2", "set0")));
        // SINTERSTORE
        assertCommandEquals(2, array("sinterstore", "inter_set1", "set1", "set3"));
        assertEquals(array("k4", "k5"), exec(array("smembers", "inter_set1")));
        assertCommandEquals(1, array("sinterstore", "inter_set1", "set1", "set3", "set2"));
        assertEquals(array("k4"), exec(array("smembers", "inter_set1")));
        assertCommandEquals(0, array("sinterstore", "inter_set1", "set1", "set0"));
        assertEquals(array(), exec(array("smembers", "inter_set1")));
        assertCommandEquals(3, array("sinterstore", "inter_set1", "set2"));
        assertEquals(array("k2", "k3", "k4"), exec(array("smembers", "inter_set1")));
        // SISMEMBER SMOVE
        assertCommandEquals(1, array("sismember", "set1", "k1"));
        assertCommandEquals(1, array("sismember", "set1", "k2"));
        assertCommandEquals(0, array("sismember", "set1", "k10"));
        assertCommandEquals(0, array("smove", "set1", "move_set1", "k10"));
        assertCommandEquals(0, array("sismember", "move_set1", "k10"));
        assertCommandEquals(1, array("sadd", "set1", "k10"));
        assertCommandEquals(1, array("smove", "set1", "move_set1", "k10"));
        assertCommandEquals(1, array("sismember", "move_set1", "k10"));
        assertCommandEquals(1, array("sadd", "set1", "k10"));
        assertCommandEquals(1, array("smove", "set1", "move_set1", "k10"));
        // SPOP SRANDMEMBER
        assertCommandEquals(3, array("sadd", "set5", "k1", "k2", "k3", "k4"));
        exec(array("srandmember", "set5"));
        exec(array("srandmember", "set5", "2"));
        exec(array("srandmember", "set5", "10"));
        exec(array("srandmember", "set5", "-2"));
        exec(array("srandmember", "set5", "0"));
        assertCommandEquals(4, array("scard", "set5"));
        exec(array("spop", "set5"));
        assertCommandEquals(3, array("scard", "set5"));
        exec(array("spop", "set5", "10"));
        assertCommandEquals(0, array("scard", "set5"));
        // SREM
        assertCommandEquals(3, array("sadd", "set5", "k1", "k2", "k3", "k4"));
        assertCommandEquals(0, array("srem", "set5", "k10"));
        assertCommandEquals(1, array("srem", "set5", "k1"));
        assertCommandEquals(2, array("srem", "set5", "k2", "k3", "k1"));
        // SSCAN
        assertCommandEquals(3, array("sadd", "set6", "k1", "k2", "k3", "k4", "k5"));

        // SUNION SUNIONSTORE

        // error
        assertCommandOK(array("set", "str1", "str1"));
        assertCommandError(array("sadd", "str1", "k1"));
        assertCommandError(array("sadd", "set1"));
        assertCommandError(array("smembers", "str1"));
        assertCommandError(array("smembers"));
        assertCommandError(array("smembers", "set1", "set2"));
        assertCommandError(array("scard", "str1"));
        assertCommandError(array("scard"));
        assertCommandError(array("scard", "set1", "set2"));
        assertCommandError(array("sdiff", "str1", "set2"));
        assertCommandError(array("sdiff", "set1", "str1"));
        assertCommandError(array("sdiff", "set1", "set2", "str1"));
        assertCommandError(array("sdiffstore", "set4"));
        assertCommandError(array("sdiffstore", "set4", "set1", "str1"));
        assertCommandError(array("sinter"));
        assertCommandError(array("sinter", "str1", "set2"));
        assertCommandError(array("sinter", "set2", "str1"));
        assertCommandError(array("sinter", "set2", "set1", "str1"));
        assertCommandError(array("sinterstore", "inter_set1"));
        assertCommandError(array("sinterstore", "inter_set1", "str1"));
        assertCommandError(array("sismember", "str1", "k1"));
        assertCommandError(array("sismember"));
        assertCommandError(array("sismember", "set1", "k1", "k2"));
        assertCommandError(array("smove", "set1", "set2"));
        assertCommandError(array("smove", "set1", "str1", "k1"));
        assertCommandError(array("smove", "set1", "set2", "k1", "k2"));
        assertCommandError(array("srandmember", "str1"));
        assertCommandError(array("srandmember"));
        assertCommandError(array("srandmember", "set5", "aaa"));
        assertCommandError(array("srandmember", "set5", "5", "11"));
        assertCommandError(array("spop", "set5", "-1"));
        assertCommandError(array("spop", "set5", "a"));
        assertCommandError(array("spop", "set5", "1", "1"));
        assertCommandError(array("spop", "set5"));
        assertCommandError(array("spop", "str1"));
        assertCommandError(array("srem", "set5"));
        assertCommandError(array("srem", "str1", "v1"));
    }

}
