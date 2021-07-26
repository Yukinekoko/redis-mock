package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.executor.*;
import com.github.zxl0714.redismock.expecptions.*;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.*;
import java.util.logging.Logger;


/**
 * Created by Xiaolu on 2015/4/20.
 */
public class CommandExecutor {

    private static final Logger LOGGER = Logger.getLogger(CommandExecutor.class.getName());

    private final Map<String, CommandDescriptor> executors;

    private final RedisBase base;

    public List<String> getSupportedCommands() {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<>();
        executors.keySet().forEach(builder::add);
        return builder.build();
    }

    public CommandExecutor(RedisBase base) {
        this.base = base;
        executors = new HashMap<>(64);
        registerDefault();
    }

    public synchronized Slice execCommand(RedisCommand command, Socket socket) throws IOException {
        Preconditions.checkArgument(command.getParameters().size() > 0);

        List<Slice> params = command.getParameters();
        String name = new String(params.get(0).data()).toLowerCase();

        CommandDescriptor descriptor = executors.get(name);
        if (descriptor == null) {
            return Response.error(String.format("ERR unknown or disabled command '%s'", name));
        }
        try {
            return descriptor.executor.execute(params.subList(1, params.size()), base, socket);
        } catch (WrongValueTypeException e) {
            return Response.error(e.getMessage());
        } catch (WrongNumberOfArgumentsException e) {
            return Response.error(String.format("ERR wrong number of arguments for '%s' command", name));
        } catch (BaseException e) {
            LOGGER.warning("未处理的异常类型 " + e.getClass().getSimpleName() + "：" + e.getMessage());
            return Response.error("ERR " + e.getMessage());
        }
    }

    public synchronized Slice execCommandFromScript(RedisCommand command) throws UnsupportedScriptCommandException, IOException {
        Preconditions.checkArgument(command.getParameters().size() > 0);
        Socket socket = getSocket();
        if (socket == null) {
            return Response.error("can not get socket");
        }
        List<Slice> params = command.getParameters();
        String name = new String(params.get(0).data()).toLowerCase();
        CommandDescriptor descriptor = executors.get(name);
        if (descriptor == null) {
            return Response.error(String.format("ERR unknown or disabled command '%s'", name));
        }
        if (!descriptor.script) {
            throw new UnsupportedScriptCommandException();
        }
        return execCommand(command, socket);
    }


    public void register(String name, Executor executor, boolean script) {
        executors.put(name, new CommandDescriptor(name, executor, script));
    }

    protected void registerDefault() {
        register("set", new SETExecutor(), true);
        register("setex", new SETEXExecutor(), true);
        register("psetex", new PSETEXExecutor(), true);
        register("setnx", new SETNXExecutor(), true);
        register("setbit", new SETBITExecutor(), true);
        register("append", new APPENDExecutor(), true);
        register("get", new GETExecutor(), true);
        register("getbit", new GETBITExecutor(), true);
        register("ttl", new TTLExecutor(), true);
        register("pttl", new PTTLExecutor(), true);
        register("expire", new EXPIREExecutor(), true);
        register("pexpire", new PEXPIREExecutor(), true);
        register("incr", new INCRExecutor(), true);
        register("incrby", new INCRBYExecutor(), true);
        register("decr", new DECRExecutor(), true);
        register("decrby", new DECRBYExecutor(), true);
        register("pfcount", new PFCOUNTExecutor(), true);
        register("pfadd", new PFADDExecutor(), true);
        register("pfmerge", new PFMERGEExecutor(), true);
        register("mget", new MGETExecutor(), true);
        register("mset", new MSETExecutor(), true);
        register("getset", new GETSETExecutor(), true);
        register("strlen", new STRLENExecutor(), true);
        register("del", new DELExecutor(), true);
        register("exists", new EXISTSExecutor(), true);
        register("expireat", new EXPIREATExecutor(), true);
        register("pexpireat", new PEXPIREATExecutor(), true);
        register("lpush", new LPUSHExecutor(), true);
        register("lpushx", new LPUSHXExecutor(), true);
        register("lrange", new LRANGEExecutor(), true);
        register("llen", new LLENExecutor(), true);
        register("lpop", new LPOPExecutor(), true);
        register("lindex", new LINDEXExecutor(), true);
        register("rpush", new RPUSHExecutor(), true);
        register("keys", new KEYSExecutor(), true);
        register("publish", new PUBLISHExecutor(), true);
        register("subscribe", new SUBSCRIBEExecutor(), false);
        register("unsubscribe", new UNSUBSCRIBEExecutor(), false);
        // TODO : (snowmeow:2021/7/26) 在lua环境下select指令仅影响脚本内的操作
        register("select", new SELECTExecutor(), true);
        register("ping", new PINGExecutor(), true);
        register("quit", new QUITExecutor(), false);
        register("eval", new EVALExecutor(), false);
    }


    protected void response(Slice message, Socket socket) {
        try {
            OutputStream outputStream = socket.getOutputStream();
            outputStream.write(message.data());
            outputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected Socket getSocket() {
        SocketAttributes socketAttributes = SocketContextHolder.getSocketAttributes();
        if (socketAttributes == null) {
            LOGGER.warning("can not get socketAttributes");
            return null;
        }
        return socketAttributes.getSocket();
    }

    protected class CommandDescriptor {

        private final String name;

        private final Executor executor;
        /** 能否在lua脚本中被执行 */
        private final boolean script;

        public CommandDescriptor(String name, Executor executor, boolean script) {
            this.name = name;
            this.script = script;
            this.executor = executor;
        }
    }

}
