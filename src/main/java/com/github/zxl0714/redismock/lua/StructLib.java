package com.github.zxl0714.redismock.lua;

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.VarArgFunction;

import java.util.logging.Logger;

/**
 * http://www.inf.puc-rio.br/~roberto/struct/
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/27
 */
public class StructLib extends VarArgFunction {

    private static final Logger LOGGER = Logger.getLogger(StructLib.class.getName());

    private static final int INIT = 0;

    private static final int PACK = 1;

    private static final int UNPACK = 2;

    private static final int SIZE = 3;

    @Override
    public Varargs invoke(Varargs args) {
        switch (opcode) {
            case INIT:
                return init();
            case PACK:
                return pack(args);
            case UNPACK:
                return unpack(args);
            case SIZE:
                return size(args);
            default:
                return NONE;
        }
    }

    public LuaValue init() {
        String[] commands = new String[] {
            "pack",
            "unpack",
            "size"};
        LuaTable t = new LuaTable();
        bind(t, StructLib.class, commands, PACK);
        env.set("struct", t);
        PackageLib.instance.LOADED.set("struct", t);
        return t;
    }

    private Varargs pack(Varargs args) {
        String expression = args.arg1().checkjstring();
        int index = 2;
        StringBuilder response = new StringBuilder();

        char temp = expression.charAt(0);
        switch (temp) {
            case 'i':
                char[] buff = new char[32];
                int number = args.arg(index).checkint();
                for (int i = 0; i < 8; i++) {
                    buff[i] = (char) (number & 0xff);
                    number >>= 8;
                }
                response.append(buff);
                break;
            default:
                LOGGER.warning("error");
        }

        return LuaValue.valueOf(response.toString());
    }

    private Varargs unpack(Varargs args) {
        return LuaValue.NIL;
    }

    private Varargs size(Varargs args) {
        return LuaValue.NIL;
    }
}
