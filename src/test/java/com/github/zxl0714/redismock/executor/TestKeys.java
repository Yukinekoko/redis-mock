package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public class TestKeys extends TestCommandExecutor {

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
    public void testKeys() throws ParseErrorException, EOFException, IOException {
        assertCommandOK(array("set", "prefix:a", "a"));
        assertCommandOK(array("set", "prefix:b", "b"));
        assertEquals(array("prefix:a","prefix:b"),
            executor.execCommand(parse(array("keys", "prefix:*")), socket).toString());
    }

    @Test
    public void testMove() throws ParseErrorException, EOFException {
        assertCommandOK(array("select", "0"));
        assertCommandEquals(0, array("move", "str1", "1"));
        assertCommandOK(array("set", "str1", "s1"));
        assertCommandOK(array("set", "str2", "s2"));
        assertCommandEquals(1, array("move", "str1", "1"));
        assertCommandNull(array("get", "str1"));
        assertCommandOK(array("select", "1"));
        assertCommandEquals("s1", array("get", "str1"));
        assertCommandOK(array("set", "str2", "ss2"));
        assertCommandEquals(0, array("move", "str12", "0"));
        assertCommandOK(array("select", "0"));
        assertCommandEquals("s2", array("get", "str2"));
        //error
        assertCommandError(array("move", "str1", "abc"));
        assertCommandError(array("move", "str1", "20"));
        assertCommandError(array("move", "str1", "-10"));
        assertCommandError(array("move", "str1", "0"));
        assertCommandError(array("move", "str1"));
        assertCommandError(array("move", "str1", "1", "1"));
    }

    @Test
    public void testPersist() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "k1", "k1"));
        assertCommandEquals(1, array("expire", "k1", "5"));
        assertCommandEquals(5, array("ttl", "k1"));
        assertCommandEquals(1, array("persist", "k1"));
        assertCommandEquals(-1, array("ttl", "k1"));
        assertCommandEquals(0, array("persist", "k1"));
        // error
        assertCommandError(array("persist"));
        assertCommandError(array("persist", "k1", "k2"));
    }

    @Test
    public void testRandomkey() throws ParseErrorException, EOFException {
        assertCommandNull(array("randomkey"));
        assertCommandOK(array("set", "k1", "k1"));
        assertCommandEquals("k1", array("randomkey"));
        // error
        assertCommandError(array("randomkey", "aaa"));
    }

    @Test
    public void testRename() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "k1", "k1"));
        assertCommandOK(array("rename", "k1", "k2"));
        assertCommandEquals("k1", array("get", "k2"));
        assertCommandNull(array("get", "k1"));
        assertCommandOK(array("set", "k1", "newk"));
        assertCommandOK(array("rename", "k1", "k2"));
        assertCommandEquals("newk", array("get", "k2"));
        // error
        assertCommandError(array("rename", "k3", "k2"));
        assertCommandError(array("rename", "k3", "k2", "k3"));
        assertCommandError(array("rename", "k3"));
    }

    @Test
    public void testRenamenx() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "k1", "k1"));
        assertCommandEquals(1, array("renamenx", "k1", "k2"));
        assertCommandOK(array("set", "k1", "nk"));
        assertCommandEquals(0, array("renamenx", "k1", "k2"));
        assertCommandEquals("nk", array("get", "k1"));
        assertCommandEquals("k1", array("get", "k2"));
        // error
        assertCommandError(array("renamenx", "k3", "k2"));
        assertCommandError(array("renamenx", "k3", "k2", "k3"));
        assertCommandError(array("renamenx", "k3"));
    }

    @Test
    public void testType() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "s1", "s1"));
        assertCommandEquals(1, array("lpush", "l1", "l1"));
        assertCommandEquals(1, array("sadd", "set1", "set1"));
        assertCommandEquals(1, array("hset", "h1", "h1", "h1"));
        // TODO : (snowmeow:2021/8/27) zset,stream
        assertCommandNONE(array("type", "s0"));
        assertCommandEquals("string", array("type", "s1"));
        assertCommandEquals("list", array("type", "l1"));
        assertCommandEquals("set", array("type", "set1"));
        assertCommandEquals("hash", array("type", "h1"));
        // error
        assertCommandError(array("type"));
        assertCommandError(array("type", "a", "b"));
    }

    @Test
    public void testScan() throws ParseErrorException, EOFException, IOException {
        assertCommandOK(array("set", "s1", "s1"));
        assertCommandOK(array("set", "s2", "s2"));
        assertCommandOK(array("set", "s3", "s3"));
        assertCommandOK(array("set", "s4", "s4"));
        assertCommandOK(array("set", "s5", "s5"));
        assertCommandOK(array("set", "s11", "s11"));

        assertEquals("*2\r\n$1\r\n0\r\n*6\r\n" +
                "$2\r\ns2\r\n$3\r\ns11\r\n" +
                "$2\r\ns3\r\n$2\r\ns4\r\n" +
                "$2\r\ns5\r\n$2\r\ns1\r\n",
            exec(array("scan", "0")));
        assertEquals("*2\r\n$1\r\n3\r\n*3\r\n" +
                "$2\r\ns2\r\n$3\r\ns11\r\n" +
                "$2\r\ns3\r\n",
            exec(array("scan", "0", "count", "3")));
        assertEquals("*2\r\n$1\r\n0\r\n*3\r\n" +
                "$2\r\ns4\r\n$2\r\ns5\r\n" +
                "$2\r\ns1\r\n",
            exec(array("scan", "3", "count", "3")));
        assertEquals("*2\r\n$1\r\n3\r\n*3\r\n" +
                "$2\r\ns2\r\n$3\r\ns11\r\n" +
                "$2\r\ns3\r\n",
            exec(array("scan", "0", "count", "3", "match", "s*")));
        assertEquals("*2\r\n$1\r\n3\r\n*1\r\n" +
                "$3\r\ns11\r\n",
            exec(array("scan", "0", "count", "3", "match", "s1*")));
        assertEquals("*2\r\n$1\r\n0\r\n*1\r\n" +
                "$2\r\ns1\r\n",
            exec(array("scan", "3", "count", "3", "match", "s1*")));

        // error
        assertCommandError(array("scan", "-1"));
        assertCommandError(array("scan", "0", "count", "0"));
        assertCommandError(array("scan", "0", "count", "-1"));
        assertCommandError(array("scan", "0", "count", "abc"));
        assertCommandError(array("scan", "0", "count"));
        assertCommandError(array("scan", "0", "baba", "3"));
        assertCommandError(array("scan", "0", "count", "3", "match"));
        assertCommandError(array("scan", "0", "count", "3", "match", "*", "abc"));
    }
}
