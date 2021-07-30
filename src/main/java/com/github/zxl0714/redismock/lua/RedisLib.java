package com.github.zxl0714.redismock.lua;

import com.github.zxl0714.redismock.*;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.github.zxl0714.redismock.expecptions.RedisCallCommandException;
import com.github.zxl0714.redismock.expecptions.UnsupportedScriptCommandException;
import com.github.zxl0714.redismock.parser.RedisToLuaReplyParser;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.PackageLib;
import org.luaj.vm2.lib.VarArgFunction;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

/**
 * TODO : (snowmeow:2021/7/27) 增加当超过maxmemory限制情况的模拟
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-22
 */
public class RedisLib extends VarArgFunction {

    private static final Logger LOGGER = Logger.getLogger(RedisLib.class.getName());

    private static final int INIT = 0;

    private static final int CALL = 1;

    private static final int PCALL = 2;

    private static final int ERROR_REPLY = 3;

    private static final int STATUS_REPLY = 4;

    private static final int SHA1HEX = 5;

    private static final int LOG = 6;



    public LuaValue init() {
        String[] commands = new String[] {
            "call",
            "pcall",
            "error_reply",
            "status_reply",
            "sha1hex",
            "log"};
        LuaTable t = new LuaTable();
        bind(t, RedisLib.class, commands, CALL);
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
            case ERROR_REPLY:
                return errorReply(args);
            case STATUS_REPLY:
                return statusReply(args);
            case SHA1HEX:
                return sha1hex(args);
            case LOG:
                return log(args);
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

    private Varargs errorReply(Varargs args) {
        return replySimpleTable(args, "err");
    }

    private Varargs statusReply(Varargs args) {
        return replySimpleTable(args, "ok");
    }

    private Varargs sha1hex(Varargs args) {
        if (args.narg() != 1) {
            return replyError("wrong number or type of arguments");
        }
        String message;
        try {
            message = args.arg1().checkjstring();
        } catch (LuaError e) {
            return replyError("wrong number or type of arguments");
        }
        return LuaValue.valueOf(sha1Digest(message));
    }

    private Varargs log(Varargs args) {
        // TODO : (snomwoew:2021/7/27) log
        return replyError("no support");
    }

    private Varargs replySimpleTable(Varargs args, String key) {
        if (args.narg() != 1) {
            return replyError("wrong number or type of arguments");
        }
        LuaValue value = args.arg1();
        if (value.isnumber()) {
            return replyError("wrong number or type of arguments");
        }
        String message;
        try {
            message = value.checkjstring();
        } catch (LuaError e) {
            return replyError("wrong number or type of arguments");
        }
        LuaTable result = new LuaTable();
        result.set(key, message);
        return result;
    }

    private String sha1Digest(String message) {
        String type = "SHA";
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(type);
            byte[] bytes = messageDigest.digest(message.getBytes());
            return bytesToHexString(bytes);
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warning("sha1Digest: NoSuchAlgorithmException - type: " + type);
            return "";
        }
    }

    private String bytesToHexString(byte[] bytes) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            int cur  = bytes[i] & 0xff;
            if (cur < 16) {
                stringBuilder.append("0");
            }
            stringBuilder.append(Integer.toHexString(cur));
        }
        return stringBuilder.toString();
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
