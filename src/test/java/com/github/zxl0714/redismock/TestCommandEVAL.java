package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.github.zxl0714.redismock.parser.RedisCommandParser;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.junit.Assert.*;
import static com.github.zxl0714.redismock.Response.*;
import static org.mockito.Mockito.when;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class TestCommandEVAL {

    private static final String CRLF = "\r\n";

    private CommandExecutor executor;

    private Socket socket;

    private RedisBase redisBase;

    private ByteArrayOutputStream outputStream;

    @Before
    public void init() throws IOException {

        executor = new CommandExecutor(new RedisBase());
        Socket socket = mock(Socket.class);
        outputStream = new ByteArrayOutputStream();
        when(socket.getOutputStream()).thenReturn(outputStream);

        SocketAttributes socketAttributes = new SocketAttributes();
        socketAttributes.setDatabaseIndex(0);
        socketAttributes.setSocket(socket);
        socketAttributes.setCommandExecutor(executor);
        SocketContextHolder.setSocketAttributes(socketAttributes);
    }

    @Test
    public void testBase() throws ParseErrorException, EOFException {
        assertCommandEquals(1, array("eval", "return 1", "0"));
        assertCommandEquals("abc", array("eval", "return 'abc'", "0"));
    }

    @Test
    public void testCallKeysAndArgv() throws ParseErrorException, EOFException, IOException {
        assertCommandOK(array("eval", "return redis.call('set', 'a', 'abc')", "0"));
        assertCommandEquals("abc", array("eval", "return redis.call('get', KEYS[1])", "1", "a"));
        assertCommandEquals("value", array("eval", "return ARGV[2]", "0", "aa", "value"));
        assertCommandOK(array("eval", "return redis.call('set', KEYS[1], ARGV[1])", "1", "cc", "wss"));
        assertCommandEquals("wss", array("eval", "return redis.call('get', 'cc')", "0"));

        List<Slice> inner = Lists.newArrayList(integer(3), Response.bulkString(new Slice("Hello World!")));
        List<Slice> list1 = Lists.newArrayList(integer(1), integer(2), Response.array(inner));
        assertEquals(Response.array(list1),
            executor.execCommand(parse(array("eval", "return {1,2,{3,'Hello World!'}}", "0")), socket));
        assertEquals(responseArray(1L, 2L, 3L, "foo"),
            executor.execCommand(parse(array(
                "eval",
                "return {1,2,3.3333,somekey='somevalue','foo',nil,'bar'}",
                "0")), socket).toString());
    }

    @Test
    public void testError() throws ParseErrorException, EOFException, IOException {
        // WrongNumberOfArguments
        assertCommandError(array("eval", "return 'abc'"));
        // keys count error
        assertCommandError(array("eval", "return KEYS[1]", "1"));
        // nil
        assertCommandNull(array("eval", "return KEYS[1]", "0", "a", "b"));
        assertCommandNull(array("eval", "return ARGV[1]", "0"));
        // call error
        assertCommandError(array("eval", "redis.call('get'); return 1", "0"));
        assertCommandEquals(1, array("eval", "redis.pcall('get'); return 1", "0"));
        assertCommandError(array("eval", "return redis.pcall('get')", "0"));
        // unknown command
        assertCommandError(array("eval", "redis.call('enknown')", "0"));
        assertCommandEquals(1, array("eval", "redis.pcall('unknown');return 1", "0"));
        // unsupported command
        assertCommandError(array("eval", "redis.call('quit')", "0"));
        assertCommandError(array("eval", "redis.call('subscribe', 'abc')", "0"));
        assertCommandError(array("eval", "redis.call('unsubscribe', 'abc')", "0"));
    }

    @Test
    public void testCall() throws ParseErrorException, EOFException {
        // get set
        assertCommandNull(array("eval", "return redis.call('get', 'ab')", "0"));
        assertCommandOK(array("eval", "return redis.call('set', 'ab', 'abc')", "0"));
        assertCommandEquals("abc", array("eval", "return redis.call('get', 'ab')", "0"));
        assertCommandOK(array("eval", "return redis.call('set', 'ab', 'abd')", "0"));
        assertCommandEquals("abd", array("eval", "return redis.call('get', 'ab')", "0"));
        assertCommandNull(array("eval", "return redis.call('get', 'ac')", "0"));
        // expire

    }

    @Test
    public void testErrorReplyAndStatusReply() throws ParseErrorException, EOFException, IOException {
        assertCommandError(array("eval", "return redis.error_reply('aaa')", "0"));
        assertCommandOK(array("eval", "return redis.status_reply('OK')", "0"));
        // error
        assertCommandError(array("eval", "return redis.status_reply('aaa', 1)", "0"));
        assertCommandError(array("eval", "return redis.status_reply(22)", "0"));
        assertCommandError(array("eval", "return redis.status_reply()", "0"));

    }

    @Test
    public void testSha1Hex() throws ParseErrorException, EOFException {
        assertCommandEquals("e0c9035898dd52fc65c41454cec9c4d2611bfb37",
            array("eval", "return redis.sha1hex('aa')", "0"));
        assertCommandEquals("356a192b7913b04c54574d18c28d46e6395428ab",
            array("eval", "return redis.sha1hex(1)", "0"));
        // error
        assertCommandError(array("eval", "return redis.sha1hex('1', 1)", "0"));
        assertCommandError(array("eval", "return redis.sha1hex()", "0"));
    }

    private static String bulkString(CharSequence param) {
        return "$" + param.length() + CRLF + param.toString() + CRLF;
    }

    private static String bulkLong(Long param) {
        return ":" + param + CRLF;
    }

    private static String array(CharSequence... params) {
        StringBuilder builder = new StringBuilder();
        builder.append('*').append(params.length).append(CRLF);
        for (CharSequence param : params) {
            if (param == null) {
                builder.append("$-1").append(CRLF);
            } else {
                builder.append(bulkString(param));
            }
        }
        return builder.toString();
    }

    /**
     * 构造响应的列表
     */
    private static String responseArray(Object... params) {
        StringBuilder builder = new StringBuilder();
        builder.append('*').append(params.length).append(CRLF);
        for (Object param : params) {
            if (param == null) {
                builder.append("$-1").append(CRLF);
            } else if (param instanceof CharSequence) {
                builder.append(bulkString((CharSequence) param));
            } else if (param instanceof Long) {
                builder.append(bulkLong((Long) param));
            } else if (param instanceof Slice) {
                builder.append(param.toString());
            }
        }
        return builder.toString();
    }

    private RedisCommand parse(String command) throws ParseErrorException, EOFException {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(command.getBytes());
        return RedisCommandParser.parse(inputStream);
    }

    private void assertCommandEquals(String expect, String command) throws ParseErrorException, EOFException {
        try {
            assertEquals(bulkString(expect), executor.execCommand(parse(command), socket).toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assertCommandEquals(long expect, String command) throws ParseErrorException, EOFException {
        try {
            assertEquals(Response.integer(expect), executor.execCommand(parse(command), socket));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assertCommandNull(String command) throws ParseErrorException, EOFException {
        try {
            assertEquals(Response.NULL, executor.execCommand(parse(command), socket));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assertCommandOK(String command) throws ParseErrorException, EOFException {
        try {
            assertEquals(Response.OK, executor.execCommand(parse(command), socket));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void assertCommandError(String command) throws ParseErrorException, EOFException {
        try {
            assertEquals('-', executor.execCommand(parse(command), socket).data()[0]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
