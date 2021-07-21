package com.github.zxl0714.redismock;

import java.net.Socket;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-19
 */
public class SocketAttributes {

    private Socket socket;

    private int databaseIndex;

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
