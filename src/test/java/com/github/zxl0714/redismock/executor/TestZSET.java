package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
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
        assertCommandError(array("zcount", "set1", "(+inf", "-inf)"));
        assertCommandError(array("zcount", "set1", "-inf", "+inf", "1"));
        assertCommandError(array("zcount", "set1", "-inf"));
        assertCommandError(array("zcount", "set1", "aaa", "2"));
    }

    @Test
    public void testZincrby() throws ParseErrorException, EOFException {
        init();
        assertCommandEquals("1", array("zscore", "set1", "z1"));
        assertCommandEquals("3", array("zincrby", "set1", "z1", "2"));
        assertCommandEquals("0", array("zincrby", "set1", "z1", "-3"));
        assertCommandEquals("-3", array("zincrby", "set1", "z2", "-3"));
        assertCommandEquals("-1.21", array("zincrby", "set1", "z1", "-1.21"));
        // error
        assertCommandError(array("zincrby", "set1", "s1", "2"));
        assertCommandError(array("zincrby", "set1", "z1"));
        assertCommandError(array("zincrby", "set1", "z1", "a"));
        assertCommandError(array("zincrby", "set1", "z1", "2", "3"));
    }

    @Test
    public void zInterstore() {

    }

    protected void init() throws ParseErrorException, EOFException {
        assertCommandOK(array("set", "s1", "s1"));
        assertCommandEquals(1, array("zadd", "zset1", "1", "z1"));
        assertCommandEquals(1, array("zadd", "zset1", "2", "z2"));
        assertCommandEquals(1, array("zadd", "zset1", "-1.11", "z3"));
    }

}
