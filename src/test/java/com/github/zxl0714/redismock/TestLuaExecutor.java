package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.expecptions.WrongNumberOfArgumentsException;
import com.github.zxl0714.redismock.lua.LuaExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-22
 */
public class TestLuaExecutor {

    private LuaExecutor luaExecutor;

    @Before
    public void init() {
        SocketAttributes socketAttributes = new SocketAttributes();
        socketAttributes.setDatabaseIndex(0);
        SocketContextHolder.setSocketAttributes(socketAttributes);

        luaExecutor = new LuaExecutor();
    }

    @Test
    public void testLua() throws WrongNumberOfArgumentsException {
        List<Slice> params = new ArrayList<>();
        Slice sc = new Slice("print(type(KEYS));print(KEYS[2]);print(ARGV[1])");
        Slice count = new Slice("2");
        Slice key = new Slice("keya");
        Slice keyb = new Slice("keyb");
        Slice arg = new Slice("arga");
        Slice argb = new Slice("argb");
        params.add(sc);
        params.add(count);
        params.add(key);
        params.add(keyb);
        params.add(arg);
        params.add(argb);

    }

}
