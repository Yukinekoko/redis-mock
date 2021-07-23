package com.github.zxl0714.redismock;

import java.net.Socket;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-19
 */
public class SocketAttributes {

    /**
     * 每个RedisWorker内共享
     * */
    private Socket socket;

    /**
     * 每个RedisWorker内共享
     * */
    private int databaseIndex;

    /**
     * 每个RedisServer内共享
     * */
    private CommandExecutor commandExecutor;

    public CommandExecutor getCommandExecutor() {
        return commandExecutor;
    }

    public void setCommandExecutor(CommandExecutor commandExecutor) {
        this.commandExecutor = commandExecutor;
    }

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    public int getDatabaseIndex() {
        return databaseIndex;
    }

    public void setDatabaseIndex(int databaseIndex) {
        this.databaseIndex = databaseIndex;
    }
}
