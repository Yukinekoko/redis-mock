package com.github.zxl0714.redismock.lua;

import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.github.zxl0714.redismock.expecptions.WrongNumberOfArgumentsException;
import com.github.zxl0714.redismock.parser.LuaToRedisReplyParser;
import org.luaj.vm2.*;
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

    static {
        ScriptEngineManager manager = new ScriptEngineManager();
        engine = (LuaScriptEngine) manager.getEngineByExtension(".lua");

        LuaTable luaEnvironment = (LuaTable) LuaThread.getGlobals();
        luaEnvironment.load(new RedisLib());
    }

    public Slice execute(List<Slice> params) throws WrongNumberOfArgumentsException, ParseErrorException {
        checkArgumentsNumberGreater(params, 1);
        CompiledScript compiledScript;
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
        Varargs scriptResult;
        try {
            compiledScript = engine.compile(params.get(0).toString());
            scriptResult = (Varargs) compiledScript.eval(bindings);
        } catch (ScriptException | LuaError e) {
            return Response.error("ERR " +e.getMessage());
        }
        return LuaToRedisReplyParser.parse((scriptResult).arg1());
    }

}
