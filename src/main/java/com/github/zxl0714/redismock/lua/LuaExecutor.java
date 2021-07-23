package com.github.zxl0714.redismock.lua;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.WrongNumberOfArgumentsException;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaThread;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.*;
import org.luaj.vm2.lib.jse.JseBaseLib;
import org.luaj.vm2.lib.jse.JseOsLib;
import org.luaj.vm2.script.LuaScriptEngine;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.*;

/**
 * lua脚本执行器,注意luaj本身可能存在线程不安全问题
 *
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-22
 */
public class LuaExecutor {

    private static final LuaScriptEngine engine;

    private final RedisBase base;

    static {
        ScriptEngineManager manager = new ScriptEngineManager();
        engine = (LuaScriptEngine) manager.getEngineByExtension(".lua");

        LuaTable luaEnvironment = new LuaTable();
        LuaThread.setGlobals(luaEnvironment);

        luaEnvironment.load(new JseBaseLib());
        luaEnvironment.load(new PackageLib());
        luaEnvironment.load(new TableLib());
        luaEnvironment.load(new StringLib());
        luaEnvironment.load(new MathLib());
        luaEnvironment.load(new CoroutineLib());
        luaEnvironment.load(new JseOsLib());

        LuaC.install();
    }

    public LuaExecutor(RedisBase base) {
        this.base = base;

    }

    public Varargs execute(List<Slice> params) throws WrongNumberOfArgumentsException {
        checkArgumentsNumberGreater(params, 1);
        CompiledScript compiledScript = null;
        Object result = null;
        int keyCount;
        try {
            keyCount = Integer.parseInt(params.get(1).toString());
        } catch (NumberFormatException e) {
            // error
            e.printStackTrace();
            return null;
        }
        checkArgumentsNumberGreater(params, 1 + keyCount);

        int argvCount = params.size() - 2 - keyCount;
        LuaValue[] keys = new LuaValue[keyCount * 2];
        LuaValue[] args = new LuaValue[argvCount * 2];


        for (int i = 2; i < params.size(); i++) {
            String param = params.get(i).toString();
            if (i - 2 < keyCount) {
                int firstIndex = (i - 2) * 2;
                keys[firstIndex] = LuaValue.valueOf(i - 1);
                keys[firstIndex + 1] = LuaValue.valueOf(param);
            } else {
                int firstIndex = (i - 2 - keyCount) * 2;
                args[firstIndex] = LuaValue.valueOf(i - 1 - keyCount);
                args[firstIndex + 1] = LuaValue.valueOf(param);
            }
        }
        LuaTable keyTable = LuaValue.tableOf(keys);
        LuaTable argTable = LuaValue.tableOf(args);
        Bindings bindings = engine.createBindings();
        if (keys.length != 0) {
            bindings.put("KEYS", keyTable);
        }
        if (args.length != 0) {
            bindings.put("ARGV", argTable);
        }

        try {
            compiledScript = engine.compile(params.get(0).toString());
            result = compiledScript.eval(bindings);
        } catch (ScriptException e) {
            e.printStackTrace();
        }
        return (Varargs) result;
    }

}
