package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

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
        assertCommandEquals("-1.11", array("zscore", "zset1", "z2"));
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

    protected void init() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "s1", "s1"));
        assertCommandEquals(1, array("zadd", "zset1", "1", "z1"));
        assertCommandEquals(1, array("zadd", "zset1", "2", "z2"));
        assertCommandEquals(1, array("zadd", "zset1", "-1.11", "z3"));
    }

}
