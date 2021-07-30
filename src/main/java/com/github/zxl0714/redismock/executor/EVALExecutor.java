package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.RedisCallCommandException;
import com.github.zxl0714.redismock.expecptions.WrongNumberOfArgumentsException;
import com.github.zxl0714.redismock.lua.RedisLuaScriptEngine;
import com.github.zxl0714.redismock.parser.LuaToRedisReplyParser;
import org.luaj.vm2.*;

import javax.script.Bindings;
import javax.script.CompiledScript;
import javax.script.ScriptException;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.checkArgumentsNumberGreater;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class EVALExecutor extends AbstractExecutor {

    private static final RedisLuaScriptEngine engine = new RedisLuaScriptEngine();

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberGreater(params, 1);
        CompiledScript compiledScript;
        int keyCount;
        try {
            keyCount = Integer.parseInt(params.get(1).toString());
        } catch (NumberFormatException e) {
            throw new WrongNumberOfArgumentsException();
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
        bindings.put("KEYS", keyTable);
        bindings.put("ARGV", argTable);
        Varargs scriptResult;
        try {
            compiledScript = engine.compile(params.get(0).toString());
            scriptResult = (Varargs) compiledScript.eval(bindings);
        } catch (ScriptException | LuaError e) {
            return Response.error("ERR " +e.getMessage());
        } catch (RedisCallCommandException e) {
            return Response.error(e.getMessage());
        }
        return LuaToRedisReplyParser.parse((scriptResult).arg1());
    }
}
