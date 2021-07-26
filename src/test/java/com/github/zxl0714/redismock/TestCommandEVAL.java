package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.executor.EVALExecutor;
import com.github.zxl0714.redismock.expecptions.BaseException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;
import static com.github.zxl0714.redismock.Response.*;
/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class TestCommandEVAL {

    private EVALExecutor executor;

    private Socket socket;

    private RedisBase redisBase;

    private CommandExecutor commandExecutor;

    @Before
    public void init() {
        executor = new EVALExecutor();
        socket = mock(Socket.class);
        redisBase = new RedisBase();
        commandExecutor = new CommandExecutor(redisBase);
        SocketAttributes socketAttributes = new SocketAttributes();
        socketAttributes.setCommandExecutor(commandExecutor);
        socketAttributes.setSocket(socket);
        SocketContextHolder.setSocketAttributes(socketAttributes);
    }

    @Test
    public void testBase() {
        assertEquals(integer(1), execute("return 1", "0"));
        assertEquals(bulkString("abc"), execute("return 'abc'", "0"));
    }

    @Test
    public void testCallKeysAndArgv() {
        assertEquals(OK, execute("return redis.call('set', 'a', 'abc')", "0"));
        assertEquals(bulkString("abc"), execute("return redis.call('get', KEYS[1])", "1", "a"));
        assertEquals(bulkString("value"), execute("return ARGV[2]", "0", "aa", "value"));
        assertEquals(OK, execute("return redis.call('set', KEYS[1], ARGV[1])", "1", "cc", "wss"));
        assertEquals(bulkString("wss"), execute("return redis.call('get', 'cc')", "0"));
    }

    private Slice execute(String... params) {
        try {
            return executor.execute(params(params), redisBase, socket);
        } catch (BaseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private List<Slice> params(String... params) {
        List<Slice> list = new ArrayList<>(params.length);
        for (String param : params) {
            list.add(new Slice(param));
        }
        return list;
    }

    private Slice bulkString(String str) {
        return Response.bulkString(new Slice(str));
    }



}
