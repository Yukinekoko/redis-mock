package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.lua.RedisLib;
import org.junit.Before;
import org.junit.Test;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

import java.net.Socket;

import static org.mockito.Mockito.*;
/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class TestRedisLib {

    private RedisLib redisLib;

    @Before
    public void init() {
        SocketAttributes socketAttributes = new SocketAttributes();
        socketAttributes.setCommandExecutor(new CommandExecutor(new RedisBase()));
        socketAttributes.setSocket(mock(Socket.class));
        SocketContextHolder.setSocketAttributes(socketAttributes);
        redisLib = new RedisLib();
    }

    @Test
    public void testCall() {
    }

    private Varargs varargs(String... params) {
        LuaValue[] luaValues = new LuaValue[params.length];
        for (int i = 0; i < luaValues.length; i++) {
            luaValues[i] = LuaValue.valueOf(params[i]);
        }
        return LuaValue.varargsOf(luaValues);
    }
}
