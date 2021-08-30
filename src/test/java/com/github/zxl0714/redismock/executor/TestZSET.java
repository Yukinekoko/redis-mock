package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import org.junit.Test;

/**
 * @author snowmeow (yuki754685421@163.com)
 * @date 2021/8/30
 */
public class TestZSET extends TestCommandExecutor {

    @Test
    public void testZadd() throws ParseErrorException, EOFException {
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
        assertCommandOK(array("set", "s1", "s1"));
        assertCommandError(array("zadd", "s1", "1", "z1"));
        assertCommandError(array("zadd", "zset1", "z1"));
        assertCommandError(array("zadd", "zset1", "aaa", "z1"));
        assertCommandError(array("zadd", "zset1", "nx", "xx", "1", "z1"));
        assertCommandError(array("zadd", "zset1", "xxx", "1", "z1"));
        assertCommandError(array("zadd", "zset1", "incr", "1", "z1", "2", "z3"));
        assertCommandError(array("zadd", "zset1", "nx", "1", "z1", "2"));

    }

}
