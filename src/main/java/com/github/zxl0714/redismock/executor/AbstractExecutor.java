package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.Slice;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @author snowmeow(snowmeow @ yuki754685421@163.com)
 * @date 2021-7-26
 */
public abstract class AbstractExecutor implements Executor {

    protected void response(Slice message, Socket socket) throws IOException {
        OutputStream outputStream = socket.getOutputStream();
        outputStream.write(message.data());
        outputStream.flush();
    }
}