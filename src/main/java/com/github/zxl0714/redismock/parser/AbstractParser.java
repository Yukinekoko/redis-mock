package com.github.zxl0714.redismock.parser;

import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021-7-23
 */
public abstract class AbstractParser {

    protected final InputStream input;

    protected AbstractParser(InputStream inputStream) {
        Preconditions.checkNotNull(inputStream);
        input = inputStream;
    }

    protected byte consumeByte() throws EOFException {
        int b;
        try {
            b = input.read();
        } catch (IOException e) {
            throw new EOFException();
        }
        if (b == -1) {
            throw new EOFException();
        }
        return (byte) b;
    }

    protected void expectByte(byte c) throws ParseErrorException, EOFException {
        if (consumeByte() != c) {
            throw new ParseErrorException();
        }
    }

    protected boolean isNumber(byte c) {
        return '0' <= c && c <= '9';
    }

}
