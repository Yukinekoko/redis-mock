package com.github.zxl0714.redismock;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-19
 */
public class SocketContextHolder {

    private static final ThreadLocal<SocketAttributes> socketAttributesThreadLocal
        = new ThreadLocal<>();

    public static void setSocketAttributes(SocketAttributes socketAttributes) {
        socketAttributesThreadLocal.set(socketAttributes);
    }

    public static SocketAttributes getSocketAttributes() {
        return socketAttributesThreadLocal.get();
    }

    public static void removeSocketAttributes() {
        socketAttributesThreadLocal.remove();
    }

}
