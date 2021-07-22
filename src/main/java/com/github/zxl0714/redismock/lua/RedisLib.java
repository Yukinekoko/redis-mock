package com.github.zxl0714.redismock.lua;

import com.github.zxl0714.redismock.CommandExecutor;
import com.github.zxl0714.redismock.RedisCommand;
import com.github.zxl0714.redismock.Slice;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.VarArgFunction;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-22
 */
public class RedisLib extends VarArgFunction {

    private static final int INIT = 0;

    private static final int CALL = 1;

    private final CommandExecutor commandExecutor;

    public RedisLib(CommandExecutor commandExecutor) {
        this.commandExecutor = commandExecutor;
    }

    public LuaValue init() {
        LuaTable t = new LuaTable(0, 1);
        bind(t, RedisLib.class, new String[]{"call"}, CALL);
        env.set("redis", t);
        PackageLib.instance.LOADED.set("redis", t);
        return t;
    }

    @Override
    public Varargs invoke(Varargs args) {
        switch (opcode) {
            case INIT:
                return init();
            case CALL:
                return call(args);
            default:
                return NONE;
        }
    }

    private Varargs call(Varargs args) {
        RedisCommand command = new RedisCommand();
        for (int i = 1; i < args.narg(); i++) {
            command.addParameter(new Slice(args.checkjstring(i)));
        }
        Slice result = commandExecutor.execCommand(command);

        return args;
    }
}
