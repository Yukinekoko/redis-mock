package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.expecptions.WrongNumberOfArgumentsException;
import org.junit.Before;
import org.junit.Test;
import org.luaj.vm2.LuaValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

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

        luaExecutor = new LuaExecutor(new RedisBase());
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

        LuaValue result = luaExecutor.execute(params);

        sc = new Slice("return 2");
        params.set(0, sc);
        luaExecutor.execute(params);
    }

    @Test
    public void testLuaInput() throws WrongNumberOfArgumentsException {
        List<Slice> params = new ArrayList<>();
        Slice script = new Slice("print('hello ' .. KEYS[1])");
        Slice key = new Slice("0");
        params.add(script);
        params.add(key);
        Scanner sc = new Scanner(System.in);
        for (int i = 0; i < 5; i++) {
            script = new Slice(sc.nextLine());
            params.set(0, script);
            luaExecutor.execute(params);
        }
    }

    public static void main(String[] args) throws WrongNumberOfArgumentsException {
        TestLuaExecutor testLuaExecutor = new TestLuaExecutor();
        testLuaExecutor.init();
        testLuaExecutor.testLuaInput();
    }
}
