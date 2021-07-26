package com.github.zxl0714.redismock.lua;

import com.github.zxl0714.redismock.*;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.github.zxl0714.redismock.expecptions.RedisCallCommandException;
import com.github.zxl0714.redismock.expecptions.UnsupportedScriptCommandException;
import com.github.zxl0714.redismock.parser.RedisToLuaReplyParser;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.VarArgFunction;

import java.io.IOException;
import java.net.Socket;
import java.util.logging.Logger;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-22
 */
public class RedisLib extends VarArgFunction {

    private static final Logger LOGGER = Logger.getLogger(RedisLib.class.getName());

    private static final int INIT = 0;

    private static final int CALL = 1;

    private static final int PCALL = 2;

    public LuaValue init() {
        LuaTable t = new LuaTable();
        bind(t, RedisLib.class, new String[]{"call", "pcall"}, CALL);
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
            case PCALL:
                return pcall(args);
            default:
                return NONE;
        }
    }

    private Varargs call(Varargs args) {
        CommandExecutor commandExecutor = getCommandExecutor();
        if (commandExecutor == null) {
            LOGGER.warning("can not get commandExecutor");
            return replyError("can not get commandExecutor");
        }
        RedisCommand command = new RedisCommand();
        for (int i = 1; i <= args.narg(); i++) {
            command.addParameter(new Slice(args.checkjstring(i)));
        }
        Slice result = null;
        try {
            result = commandExecutor.execCommandFromScript(command);
            if (result.data()[0] == '-') {
                throw new RedisCallCommandException(new String(result.data(), 1, result.length() - 1));
            }
            return RedisToLuaReplyParser.parse(result);
        } catch (IOException e) {
            String message = "redis.call IO error: " + e.getMessage();
            LOGGER.warning(message);
            throw new RedisCallCommandException(message);
        } catch (ParseErrorException e) {
            String message = "redis.call reply parse error: " + result.toString();
            LOGGER.warning(message);
            throw new RedisCallCommandException(message);
        } catch (UnsupportedScriptCommandException e) {
            throw new RedisCallCommandException("ERR This Redis command is not allowed from scripts");
        }
    }

    private Varargs pcall(Varargs args) {
        CommandExecutor commandExecutor = getCommandExecutor();
        if (commandExecutor == null) {
            LOGGER.warning("can not get commandExecutor");
            return replyError("can not get commandExecutor");
        }
        RedisCommand command = new RedisCommand();
        for (int i = 1; i <= args.narg(); i++) {
            command.addParameter(new Slice(args.checkjstring(i)));
        }
        Slice result = null;
        try {
            result = commandExecutor.execCommandFromScript(command);
            return RedisToLuaReplyParser.parse(result);
        } catch (IOException e) {
            String message = "redis.call IO error: " + e.getMessage();
            LOGGER.warning(message);
            return replyError(message);
        } catch (ParseErrorException e) {
            String message = "redis.call reply parse error: " + result.toString();
            LOGGER.warning(message);
            return replyError(message);
        } catch (UnsupportedScriptCommandException e) {
            return replyError("ERR This Redis command is not allowed from scripts");
        }
    }

    private CommandExecutor getCommandExecutor() {
        SocketAttributes socketAttributes = SocketContextHolder.getSocketAttributes();
        if (socketAttributes == null) {
            LOGGER.warning("can not get socketAttributes");
            return null;
        }
        return socketAttributes.getCommandExecutor();
    }

    private LuaValue replyError(String message) {
        LuaTable table = new LuaTable();
        table.set("err", message);
        return table;
    }
}
