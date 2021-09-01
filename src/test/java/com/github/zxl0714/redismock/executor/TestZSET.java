package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public class TestZSET extends TestCommandExecutor {

    @Test
    public void testZadd() throws ParseErrorException, EOFException, IOException {
        assertCommandNull(array("zscore", "zset1", "z1"));
        assertCommandEquals(1, array("zadd", "zset1", "1", "z1"));
        assertCommandEquals("1", array("zscore", "zset1", "z1"));
        assertCommandEquals(0, array("zadd", "zset1", "2", "z1"));
        assertCommandEquals("2", array("zscore", "zset1", "z1"));
        assertCommandEquals(1, array("zadd", "zset1", "-1.11", "z2"));
        assertCommandEquals("-1.1100000000000001", array("zscore", "zset1", "z2"));
        assertCommandEquals(2, array("zadd", "zset1", "-1.11", "z2", "3", "z3", "4", "z4"));
        assertCommandEquals("4", array("zadd", "zset1", "incr", "2", "z1"));
        assertCommandEquals(0, array("zadd", "zset1", "xx", "10", "z1"));
        assertCommandEquals("10", array("zscore", "zset1", "z1"));
        assertCommandEquals(1, array("zadd", "zset1", "nx", "2", "z1", "5", "znx"));
        assertCommandEquals("10", array("zscore", "zset1", "z1"));
        assertCommandEquals("5", array("zscore", "zset1", "znx"));
        assertCommandEquals(2, array("zadd", "zset1", "ch", "11", "z1", "5", "zch"));
        assertCommandEquals("21", array("zadd", "zset1", "incr", "10", "z1"));
        // error
        exec(array("hset", "hash1", "h1", "z1"));
        assertCommandError(array("zadd", "hash1", "1", "h2"));
        assertCommandOK(array("set", "s1", "s1"));
        assertCommandError(array("zadd", "s1", "1", "z1"));
        assertCommandError(array("zadd", "zset1", "z1"));
        assertCommandError(array("zadd", "zset1", "aaa", "z1"));
        assertCommandError(array("zadd", "zset1", "nx", "xx", "1", "z1"));
        assertCommandError(array("zadd", "zset1", "xxx", "1", "z1"));
        assertCommandError(array("zadd", "zset1", "incr", "1", "z1", "2", "z3"));
        assertCommandError(array("zadd", "zset1", "nx", "1", "z1", "2"));

    }

    @Test
    public void testZcard() throws ParseErrorException, EOFException {
        init();
        assertCommandEquals(0, array("zcard", "zset0"));
        assertCommandEquals(3, array("zcard", "zset1"));
        // error
        assertCommandError(array("zcard", "s1"));
        assertCommandError(array("zcard"));
        assertCommandError(array("zcard", "zset1", "zset2"));
    }

    @Test
    public void testZcount() throws ParseErrorException, EOFException {
        init();
        assertCommandEquals(0, array("zcount", "zset0", "-inf", "+inf"));
        assertCommandEquals(3, array("zcount", "zset1", "-inf", "+inf"));
        assertCommandEquals(3, array("zcount", "zset1", "(-inf", "(+inf"));
        assertCommandEquals(3, array("zcount", "zset1", "-1.11", "2"));
        assertCommandEquals(2, array("zcount", "zset1", "(-1.11", "2"));
        assertCommandEquals(1, array("zcount", "zset1", "(-1.11", "(2"));
        assertCommandEquals(0, array("zcount", "zset1", "+inf", "+inf"));
        // error
        assertCommandError(array("zcount", "s1", "+inf", "-inf"));
        assertCommandError(array("zcount", "zset1", "(+inf", "-inf)"));
        assertCommandError(array("zcount", "zset1", "-inf", "+inf", "1"));
        assertCommandError(array("zcount", "zset1", "-inf"));
        assertCommandError(array("zcount", "zset1", "aaa", "2"));
    }

    @Test
    public void testZincrby() throws ParseErrorException, EOFException {
        init();
        assertCommandEquals("1", array("zscore", "zset1", "z1"));
        assertCommandEquals("3", array("zincrby", "zset1", "2", "z1"));
        assertCommandEquals("0", array("zincrby", "zset1", "-3", "z1"));
        assertCommandEquals("-1", array("zincrby", "zset1", "-3", "z2"));
        assertCommandEquals("-1.20999999999999996", array("zincrby", "zset1", "-1.21", "z1"));
        // error
        assertCommandError(array("zincrby", "s1", "2", "s1"));
        assertCommandError(array("zincrby", "zset1", "z1"));
        assertCommandError(array("zincrby", "zset1", "a", "z1"));
        assertCommandError(array("zincrby", "zset1", "2", "z1", "3"));
    }

    @Test
    public void testZinterstore() throws ParseErrorException, EOFException {
        init();
        assertCommandEquals(3, array("zadd", "zset2", "2", "z5",
            "-3", "z2", "3.1", "z3"));
        assertCommandEquals(2, array("zinterstore", "inter1", "2", "zset1", "zset2"));
        assertCommandEquals("-1", array("zscore", "inter1", "z2"));
        assertCommandEquals("1.98999999999999999", array("zscore", "inter1", "z3"));
        assertCommandEquals(2, array("zinterstore", "inter2", "2", "zset1", "zset2", "weights",
            "1", "3"));
        assertCommandEquals("-7", array("zscore", "inter2", "z2"));
        assertCommandEquals(2, array("zinterstore", "inter3", "2", "zset1", "zset2", "aggregate", "min"));
        assertCommandEquals("-3", array("zscore", "inter3", "z2"));
        assertCommandEquals("-1.1100000000000001", array("zscore", "inter3", "z3"));

        // error
        assertCommandError(array("zinterstore", "inter3", "2", "s1", "zset2", "aggregate", "min"));
        assertCommandError(array("zinterstore", "inter3", "3", "zset1", "zset2"));
        assertCommandError(array("zinterstore", "inter3", "3", "zset1", "zset2", "weights", "a", "a"));
        assertCommandError(array("zinterstore", "inter3", "3", "zset1", "zset2", "weights", "1"));
        assertCommandError(array("zinterstore", "inter3", "3", "zset1", "zset2", "aggregate", "aaa"));

    }
    
    @Test
    public void testZunionstore() throws ParseErrorException, EOFException {
        init();
        assertCommandEquals(3, array("zadd", "zset2", "2", "z5",
            "-3", "z2", "3.1", "z3"));
        assertCommandEquals(4, array("zunionstore", "union1", "2", "zset1", "zset2"));
        assertCommandEquals("-1", array("zscore", "union1", "z2"));
        assertCommandEquals("1.98999999999999999", array("zscore", "union1", "z3"));
        assertCommandEquals("1", array("zscore", "union1", "z1"));
        assertCommandEquals("2", array("zscore", "union1", "z5"));
        assertCommandEquals(4, array("zunionstore", "union2", "2", "zset1", "zset2", "weights",
            "1", "3"));
        assertCommandEquals("-7", array("zscore", "union2", "z2"));
        assertCommandEquals("6", array("zscore", "union2", "z5"));
        assertCommandEquals(4, array("zunionstore", "union3", "2", "zset1", "zset2", "aggregate", "min"));
        assertCommandEquals("-3", array("zscore", "union3", "z2"));
        assertCommandEquals("-1.1100000000000001", array("zscore", "union3", "z3"));
        assertCommandEquals("2", array("zscore", "union3", "z5"));
        // error
        assertCommandError(array("zunionstore", "union3", "2", "s1", "zset2", "aggregate", "min"));
        assertCommandError(array("zunionstore", "union3", "3", "zset1", "zset2"));
        assertCommandError(array("zunionstore", "union3", "3", "zset1", "zset2", "weights", "a", "a"));
        assertCommandError(array("zunionstore", "union3", "3", "zset1", "zset2", "weights", "1"));
        assertCommandError(array("zunionstore", "union3", "3", "zset1", "zset2", "aggregate", "aaa"));
    }

    @Test
    public void testZrange() throws ParseErrorException, EOFException, IOException {
        init();
        Assert.assertEquals(array("z3", "z1", "z2"), exec(array("zrange", "zset1", "0", "2")));
        Assert.assertEquals(array("z3", "z1", "z2"), exec(array("zrange", "zset1", "0", "5")));
        Assert.assertEquals(array("z3", "z1"), exec(array("zrange", "zset1", "0", "1")));
        Assert.assertEquals("*0\r\n", exec(array("zrange", "zset1", "-1", "0")));
        Assert.assertEquals(array("z3"), exec(array("zrange", "zset1", "0", "-3")));
        Assert.assertEquals(array("z3", "z1"), exec(array("zrange", "zset1", "-3", "-2")));
        Assert.assertEquals(array("z1", "1", "z2", "2"), exec(array("zrange", "zset1", "-2", "-1", "withscores")));
        assertCommandEquals(1, array("zadd", "zset1", "1", "zz1"));
        Assert.assertEquals(array("zz1", "1", "z2", "2"), exec(array("zrange", "zset1", "-2", "-1", "withscores")));
        // error
        assertCommandError(array("zrange", "s1", "0", "2"));
        assertCommandError(array("zrange", "zset1", "abc", "2"));
        assertCommandError(array("zrange", "zset1", "0"));
        assertCommandError(array("zrange", "zset1", "0", "1", "2"));
    }
    
    @Test
    public void testZrevrange() throws ParseErrorException, EOFException, IOException {
        init();
        Assert.assertEquals(array("z2", "z1", "z3"), exec(array("zrevrange", "zset1", "0", "2")));
        Assert.assertEquals(array("z2", "z1", "z3"), exec(array("zrevrange", "zset1", "0", "5")));
        Assert.assertEquals(array("z2", "z1"), exec(array("zrevrange", "zset1", "0", "1")));
        Assert.assertEquals("*0\r\n", exec(array("zrevrange", "zset1", "-1", "0")));
        Assert.assertEquals(array("z2"), exec(array("zrevrange", "zset1", "0", "-3")));
        Assert.assertEquals(array("z2", "z1"), exec(array("zrevrange", "zset1", "-3", "-2")));
        Assert.assertEquals(array("z1", "1", "z3", "-1.1100000000000001"), exec(array("zrevrange", "zset1", "-2", "-1", "withscores")));
        assertCommandEquals(1, array("zadd", "zset1", "1", "zz1"));
        Assert.assertEquals(array("z1", "1", "z3", "-1.1100000000000001"), exec(array("zrevrange", "zset1", "-2", "-1", "withscores")));
        // error
        assertCommandError(array("zrevrange", "s1", "0", "2"));
        assertCommandError(array("zrevrange", "zset1", "abc", "2"));
        assertCommandError(array("zrevrange", "zset1", "0"));
        assertCommandError(array("zrevrange", "zset1", "0", "1", "2"));
    }

    @Test
    public void testZrank() throws ParseErrorException, EOFException {
        init();
        assertCommandNull(array("zrank", "zset1", "z5"));
        assertCommandNull(array("zrank", "zset0", "z5"));
        assertCommandEquals(0, array("zrank", "zset1", "z3"));
        assertCommandEquals(2, array("zrank", "zset1", "z2"));
        assertCommandEquals(1, array("zrank", "zset1", "z1"));
        // error
        assertCommandError(array("zrank", "s1", "z1"));
        assertCommandError(array("zrank", "zset1"));
        assertCommandError(array("zrank", "zset1", "s1", "s2"));
    }

    @Test
    public void testzremrangebyrank() throws ParseErrorException, EOFException {
        init();
        assertCommandEquals(0, array("zremrangebyrank", "zset0", "0", "-1"));
        assertCommandEquals(3, array("zremrangebyrank", "zset1", "0", "-1"));
        init();
        assertCommandEquals(1, array("zremrangebyrank", "zset1", "0", "0"));
        assertCommandEquals(2, array("zremrangebyrank", "zset1", "0", "-1"));
        assertCommandEquals(0, array("zremrangebyrank", "zset1", "0", "-1"));
        init();
        assertCommandEquals(0, array("zremrangebyrank", "zset1", "0", "-10"));
        // error
        assertCommandError(array("zremrangebyrank", "s1", "0", "1"));
        assertCommandError(array("zremrangebyrank", "zset1", "0"));
        assertCommandError(array("zremrangebyrank", "zset1", "0", "1", "2"));
        assertCommandError(array("zremrangebyrank", "zset1", "0", "aaa"));
    }

    @Test
    public void testZrevrank() throws ParseErrorException, EOFException {
        init();
        assertCommandNull(array("zrevrank", "zset1", "z5"));
        assertCommandNull(array("zrevrank", "zset0", "z5"));
        assertCommandEquals(2, array("zrevrank", "zset1", "z3"));
        assertCommandEquals(0, array("zrevrank", "zset1", "z2"));
        assertCommandEquals(1, array("zrevrank", "zset1", "z1"));
        // error
        assertCommandError(array("zrevrank", "s1", "z1"));
        assertCommandError(array("zrevrank", "zset1"));
        assertCommandError(array("zrevrank", "zset1", "s1", "s2"));
    }

    @Test
    public void testZrem() throws ParseErrorException, EOFException {
        init();
        assertCommandEquals(0, array("zrem", "zset0", "z1", "z2"));
        assertCommandEquals(1, array("zrem", "zset1", "z1", "z4"));
        assertCommandEquals(1, array("zrem", "zset1", "z1", "z2"));
        // error
        assertCommandError(array("zrem", "s1", "z1"));
        assertCommandError(array("zrem", "zset1"));

    }

    @Test
    public void testZscan() throws ParseErrorException, EOFException, IOException {
        init();
        assertEquals("*2\r\n$1\r\n0\r\n*-1\r\n",
            exec(array("zscan", "zset0", "0")));
        assertCommandEquals(3, array("zadd", "zset1", "4" ,"z4", "5", "z5", "6", "z6"));
        assertEquals("*2\r\n$1\r\n0\r\n*12\r\n" +
                "$2\r\nz3\r\n$19\r\n-1.1100000000000001\r\n" +
                "$2\r\nz1\r\n$1\r\n1\r\n" +
                "$2\r\nz2\r\n$1\r\n2\r\n" +
                "$2\r\nz4\r\n$1\r\n4\r\n" +
                "$2\r\nz5\r\n$1\r\n5\r\n" +
                "$2\r\nz6\r\n$1\r\n6\r\n",
            exec(array("zscan", "zset1", "0")));
        assertEquals("*2\r\n$1\r\n3\r\n*6\r\n" +
                "$2\r\nz3\r\n$19\r\n-1.1100000000000001\r\n" +
                "$2\r\nz1\r\n$1\r\n1\r\n" +
                "$2\r\nz2\r\n$1\r\n2\r\n",
            exec(array("zscan", "zset1", "0", "count", "3")));
        assertEquals("*2\r\n$1\r\n0\r\n*6\r\n" +
                "$2\r\nz4\r\n$1\r\n4\r\n" +
                "$2\r\nz5\r\n$1\r\n5\r\n" +
                "$2\r\nz6\r\n$1\r\n6\r\n",
            exec(array("zscan", "zset1", "3", "count", "3")));
        // match
        assertEquals("*2\r\n$1\r\n5\r\n*2\r\n" +
                "$2\r\nz1\r\n$1\r\n1\r\n",
            exec(array("zscan", "zset1", "0", "count", "5", "match", "z1")));
        assertEquals("*2\r\n$1\r\n3\r\n*6\r\n" +
                "$2\r\nz3\r\n$19\r\n-1.1100000000000001\r\n" +
                "$2\r\nz1\r\n$1\r\n1\r\n" +
                "$2\r\nz2\r\n$1\r\n2\r\n",
            exec(array("zscan", "zset1", "0", "count", "3", "match", "z*")));
        // error
        assertCommandError(array("zscan", "s1", "0"));
        assertCommandError(array("zscan", "zset1"));
        assertCommandError(array("zscan", "zset1", "-1"));
        assertCommandError(array("zscan", "zset1", "abc"));
        assertCommandError(array("zscan", "zset1", "0", "count", "0"));
        assertCommandError(array("zscan", "zset1", "0", "count", "-1"));
        assertCommandError(array("zscan", "zset1", "0", "count", "abc"));
        assertCommandError(array("zscan", "zset1", "0", "count"));
        assertCommandError(array("zscan", "zset1", "0", "baba", "0"));
        assertCommandError(array("zscan", "zset1", "0", "count", "1", "match"));
        assertCommandError(array("zscan", "zset1", "0", "count", "1", "match", "*", "a"));

    }

    @Test
    public void testZrangebyscore() throws ParseErrorException, EOFException, IOException {
        init();
        assertEquals(array("z3", "z1", "z2"), exec(array("zrangebyscore", "zset1", "-inf",  "+inf")));
        assertEquals(array(), exec(array("zrangebyscore", "zset0", "-inf",  "+inf")));
        assertEquals(array("z3"), exec(array("zrangebyscore", "zset1", "-inf",  "+inf", "limit", "0", "1")));
        assertEquals(array("z1"), exec(array("zrangebyscore", "zset1", "-inf",  "+inf", "limit", "1", "1")));
        assertEquals(array("z1", "z2"), exec(array("zrangebyscore", "zset1", "-inf",  "+inf", "limit", "1", "2")));
        assertEquals(array(), exec(array("zrangebyscore", "zset1", "-inf",  "+inf", "limit", "10", "2")));
        assertEquals(array(), exec(array("zrangebyscore", "zset1", "-inf",  "+inf", "limit", "-1", "2")));
        assertEquals(array("z3", "z1", "z2"), exec(array("zrangebyscore", "zset1", "-1.11",  "2")));
        assertEquals(array("z1", "z2"), exec(array("zrangebyscore", "zset1", "(-1.11",  "2")));
        assertEquals(array("z1", "1"), exec(array("zrangebyscore", "zset1", "-inf",  "+inf", "limit", "1", "1", "withscores")));
        // error
        assertCommandError(array("zrangebyscore", "s1", "-inf", "+inf"));
        assertCommandError(array("zrangebyscore", "zset1", "-inf"));
        assertCommandError(array("zrangebyscore", "zset1", "-inf", "+xxx"));
        assertCommandError(array("zrangebyscore", "zset1", "-inf", "+inf", "limit", "aaa", "1"));
        assertCommandError(array("zrangebyscore", "zset1", "-inf", "+inf", "limit", "1"));
        assertCommandError(array("zrangebyscore", "zset1", "-inf", "+inf", "limit", "1", "1", "xxx"));
    }
    
    @Test
    public void testZrevrangebyscore() throws ParseErrorException, EOFException, IOException {
        init();
        assertEquals(array(), exec(array("zrevrangebyscore", "zset1", "-inf",  "+inf")));
        assertEquals(array("z2", "z1", "z3"), exec(array("zrevrangebyscore", "zset1", "+inf",  "-inf")));
        assertEquals(array(), exec(array("zrevrangebyscore", "zset0", "-inf",  "+inf")));
        assertEquals(array("z2"), exec(array("zrevrangebyscore", "zset1", "+inf",  "-inf", "limit", "0", "1")));
        assertEquals(array("z1"), exec(array("zrevrangebyscore", "zset1", "+inf",  "-inf", "limit", "1", "1")));
        assertEquals(array("z1", "z3"), exec(array("zrevrangebyscore", "zset1", "+inf",  "-inf", "limit", "1", "2")));
        assertEquals(array(), exec(array("zrevrangebyscore", "zset1", "+inf",  "-inf", "limit", "10", "2")));
        assertEquals(array(), exec(array("zrevrangebyscore", "zset1", "+inf",  "-inf", "limit", "-1", "2")));
        assertEquals(array("z2", "z1", "z3"), exec(array("zrevrangebyscore", "zset1", "2",  "-1.11")));
        assertEquals(array("z1", "z3"), exec(array("zrevrangebyscore", "zset1", "(2",  "-1.11")));
        assertEquals(array("z1", "1"), exec(array("zrevrangebyscore", "zset1", "+inf",  "-inf", "limit", "1", "1", "withscores")));
        // error
        assertCommandError(array("zrevrangebyscore", "s1", "-inf", "+inf"));
        assertCommandError(array("zrevrangebyscore", "zset1", "-inf"));
        assertCommandError(array("zrevrangebyscore", "zset1", "-inf", "+xxx"));
        assertCommandError(array("zrevrangebyscore", "zset1", "-inf", "+inf", "limit", "aaa", "1"));
        assertCommandError(array("zrevrangebyscore", "zset1", "-inf", "+inf", "limit", "1"));
        assertCommandError(array("zrevrangebyscore", "zset1", "-inf", "+inf", "limit", "1", "1", "xxx"));
    }

    @Test
    public void testZremrangebyscore() throws ParseErrorException, EOFException {
        init();
        assertCommandEquals(0, array("zremrangebyscore", "zset0", "-inf", "+inf"));
        assertCommandEquals(3, array("zremrangebyscore", "zset1", "-inf", "+inf"));
        init();
        assertCommandEquals(1, array("zremrangebyscore", "zset1", "-1.11", "(1"));
        assertCommandEquals(2, array("zremrangebyscore", "zset1", "1", "2"));
        // error
        assertCommandError(array("zremrangebyscore", "s1", "-inf", "+inf"));
        assertCommandError(array("zremrangebyscore", "zset1", "-inf", "+xxx"));
        assertCommandError(array("zremrangebyscore", "zset1", "-inf", "+inf", "xxx"));
    }


    protected void init() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "s1", "s1"));
        assertCommandEquals(1, array("zadd", "zset1", "1", "z1"));
        assertCommandEquals(1, array("zadd", "zset1", "2", "z2"));
        assertCommandEquals(1, array("zadd", "zset1", "-1.11", "z3"));
    }
}
