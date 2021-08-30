package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public class TestSet extends TestCommandExecutor {

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
        assertCommandEquals(4, array("sadd", "set5", "k1", "k2", "k3", "k4"));
        exec(array("srandmember", "set5"));
        exec(array("srandmember", "set5", "2"));
        exec(array("srandmember", "set5", "10"));
        exec(array("srandmember", "set5", "-2"));
        exec(array("srandmember", "set5", "0"));
        assertCommandEquals(4, array("scard", "set5"));
        exec(array("spop", "set5"));
        assertCommandEquals(3, array("scard", "set5"));
        exec(array("spop", "set5", "10"));
        assertCommandNull(array("spop", "set5"));
        assertCommandNull(array("srandmember", "set5"));
        assertCommandEquals(0, array("scard", "set5"));
        // SREM
        assertCommandEquals(4, array("sadd", "set5", "k1", "k2", "k3", "k4"));
        assertCommandEquals(0, array("srem", "set5", "k10"));
        assertCommandEquals(1, array("srem", "set5", "k1"));
        assertCommandEquals(2, array("srem", "set5", "k2", "k3", "k1"));
        // SSCAN
        assertEquals("*2\r\n$1\r\n0\r\n*-1\r\n",
            exec(array("sscan", "set6", "0")));
        assertCommandEquals(5, array("sadd", "set6", "k1", "k2", "k3", "k4", "k5"));
        assertEquals("*2\r\n$1\r\n3\r\n*3\r\n" +
                "$2\r\nk2\r\n$2\r\nk3\r\n" +
                "$2\r\nk4\r\n",
            exec(array("sscan", "set6", "0", "count", "3")));
        assertEquals("*2\r\n$1\r\n0\r\n*2\r\n" +
                "$2\r\nk5\r\n$2\r\nk1\r\n",
            exec(array("sscan", "set6", "3", "count", "3")));
        assertEquals("*2\r\n$1\r\n0\r\n*1\r\n" +
                "$2\r\nk1\r\n",
            exec(array("sscan", "set6", "0", "count", "5", "match", "k1")));
        assertEquals("*2\r\n$1\r\n0\r\n*1\r\n" +
                "$2\r\nk1\r\n",
            exec(array("sscan", "set6", "0", "count", "5", "match", "*1")));
        // SUNION SUNIONSTORE
        assertEquals(array("k2", "k3", "k4"), exec(array("sunion", "set2")));
        assertEquals(array("k2", "k3", "k4"), exec(array("sunion", "set2", "set0")));
        assertEquals(array("k2", "k3", "k4", "k5", "k7"), exec(array("sunion", "set2", "set3")));
        assertEquals(array("k1", "k2", "k3", "k4", "k5", "k6", "k7"), exec(array("sunion", "set1", "set2", "set3")));
        assertEquals(array(), exec(array("sunion", "set0", "set00", "set000")));
        assertCommandEquals(0, array("sunionstore", "union_set1", "set0", "set00"));
        assertCommandEquals(5, array("sunionstore", "union_set1", "set2", "set3"));
        assertCommandEquals(7, array("sunionstore", "union_set1", "set2", "set3", "set1"));
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
        assertCommandError(array("spop", "str1"));
        assertCommandError(array("srem", "set5"));
        assertCommandError(array("srem", "str1", "v1"));
        assertCommandError(array("sunion", "str1", "set1"));
        assertCommandError(array("sunion", "set1", "str1"));
        assertCommandError(array("sunion"));
        assertCommandError(array("sunionstore", "union_set1", "str1"));
        assertCommandError(array("sunionstore", "union_set1"));
        assertCommandError(array("sscan", "str1", "0"));
        assertCommandError(array("sscan", "set1"));
        assertCommandError(array("sscan", "set1", "-1"));
        assertCommandError(array("sscan", "set1", "abc"));
        assertCommandError(array("sscan", "set1", "0", "count", "0"));
        assertCommandError(array("sscan", "set1", "0", "count", "-1"));
        assertCommandError(array("sscan", "set1", "0", "count", "abc"));
        assertCommandError(array("sscan", "set1", "0", "count"));
        assertCommandError(array("sscan", "set1", "0", "baba", "0"));
        assertCommandError(array("sscan", "set1", "0", "count", "1", "match"));
        assertCommandError(array("sscan", "set1", "0", "count", "1", "match", "*", "a"));
    }
}
