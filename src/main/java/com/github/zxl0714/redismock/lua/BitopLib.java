package com.github.zxl0714.redismock.lua;

import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.VarArgFunction;

import java.util.logging.Logger;

/**
 * TODO
 * 当输入的数字超过32位时，产生的结果将与redis中的结果不一致
 * http://bitop.luajit.org/api.html
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/27
 */
public class BitopLib extends VarArgFunction {

    private static final Logger LOGGER = Logger.getLogger(BitopLib.class.getName());

    private static final int INIT = 0;

    private static final int TOBIT = 1;

    private static final int TOHEX = 2;

    private static final int BNOT = 3;

    private static final int BOR = 4;

    private static final int BAND = 5;

    private static final int BXOR = 6;

    private static final int LSHIFT = 7;

    private static final int RSHIFT = 8;

    private static final int ARSHIFT = 9;

    private static final int ROL = 10;

    private static final int ROR = 11;

    private static final int BSWAP = 12;

    @Override
    public Varargs invoke(Varargs args) {
        switch (opcode) {
            case INIT:
                return init();
            case TOBIT:
                return toBit(args);
            case TOHEX:
                return toHex(args);
            case BNOT:
                return bNot(args);
            case BOR:
                return bOr(args);
            case BAND:
                return bAnd(args);
            case BXOR:
                return bXor(args);
            case LSHIFT:
                return lShift(args);
            case RSHIFT:
                return rShift(args);
            case ARSHIFT:
                return arShift(args);
            case ROL:
                return rol(args);
            case ROR:
                return ror(args);
            case BSWAP:
                return bSwap(args);
            default:
                return NONE;
        }
    }

    public LuaValue init() {
        String[] commands = new String[] {
            "tobit",
            "tohex",
            "bnot",
            "bor",
            "band",
            "bxor",
            "lshift",
            "rshift",
            "arshift",
            "rol",
            "ror",
            "bswap"};
        LuaTable t = new LuaTable();
        bind(t, BitopLib.class, commands, TOBIT);
        env.set("bit", t);
        PackageLib.instance.LOADED.set("bit", t);
        return t;
    }

    private Varargs toBit(Varargs args) {
        int input = args.arg1().checkint();
        for (int i = 1; i <= args.narg(); i++) {
            args.arg(i).checkint();
        }
        return LuaValue.valueOf(input);
    }

    private Varargs toHex(Varargs args) {
        int input = args.arg1().checkint();
        String hex = Integer.toHexString(input);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 8 - hex.length(); i++) {
            sb.append('0');
        }
        sb.append(hex);
        return LuaValue.valueOf(sb.toString());
    }

    private Varargs bNot(Varargs args) {
        int input = args.arg1().checkint();
        return LuaValue.valueOf(~input);
    }

    private Varargs bOr(Varargs args) {
        return LuaValue.NIL;
    }

    private Varargs bAnd(Varargs args) {
        return LuaValue.NIL;
    }

    private Varargs bXor(Varargs args) {
        return LuaValue.NIL;
    }

    private Varargs lShift(Varargs args) {
        return LuaValue.NIL;
    }

    private Varargs rShift(Varargs args) {
        return LuaValue.NIL;
    }

    private Varargs arShift(Varargs args) {
        return LuaValue.NIL;
    }

    private Varargs rol(Varargs args) {
        return LuaValue.NIL;
    }

    private Varargs ror(Varargs args) {
        return LuaValue.NIL;
    }

    private Varargs bSwap(Varargs args) {
        return LuaValue.NIL;
    }
}
