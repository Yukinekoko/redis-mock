package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.github.zxl0714.redismock.lua.ReplyConverter;
import org.junit.Test;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-22
 */
public class TestReplyConverter {

    @Test
    public void testRedis2Lua() throws EOFException, ParseErrorException {
        String strOK = "+OK\r\n";
        String strPONG = "+PONG\r\n";
        String strNULL = "$-1\r\n";
        String strError = "-ERR message\r\n";
        String strString1 = "$5\r\nhello\r\n";
        String strString2 = "$0\r\n";
        String strArray = "*4\r\n$5\r\nhello\r\n$-1\r\n:2\r\n$2\r\nab\r\n";
        String strNullArray = "*-1\r\n";
        String strNestArray = "*2\r\n:2\r\n*2\r\n$5\r\nhello\r\n:2\r\n";
        String strNumber = ":1\r\n";

        ReplyConverter.redis2Lua(new Slice(strOK));
        ReplyConverter.redis2Lua(new Slice(strPONG));
        ReplyConverter.redis2Lua(new Slice(strNULL));
        ReplyConverter.redis2Lua(new Slice(strError));
        ReplyConverter.redis2Lua(new Slice(strString1));
        ReplyConverter.redis2Lua(new Slice(strString2));
        ReplyConverter.redis2Lua(new Slice(strArray));
        ReplyConverter.redis2Lua(new Slice(strNullArray));
        ReplyConverter.redis2Lua(new Slice(strNestArray));
        ReplyConverter.redis2Lua(new Slice(strNumber));
    }
}
