package com.github.zxl0714.redismock.parser;

import com.github.zxl0714.redismock.RedisCommand;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.EOFException;
import com.github.zxl0714.redismock.expecptions.ParseErrorException;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.InputStream;
import java.util.logging.Logger;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisCommandParser extends AbstractParser {

    private static final Logger LOGGER = Logger.getLogger(RedisCommandParser.class.getName());

    public RedisCommandParser(InputStream messageInput) {
        super(messageInput);

    }

    /**
     * 读取的byte中只能出现数字，在parseCommand()中使用
     * */
    private long consumePositiveLong() throws ParseErrorException {
        byte c;
        long ret = 0;
        boolean hasLong = false;
        while (true) {
            try {
                c = consumeByte();
            } catch (EOFException e) {
                throw new ParseErrorException();
            }
            if (c == '\r') {
                break;
            }
            if (!isNumber(c)) {
                throw new ParseErrorException();
            }
            ret = ret * 10 + c - '0';
            hasLong = true;
        }
        if (!hasLong) {
            throw new ParseErrorException();
        }
        return ret;
    }

    private Slice consumeSlice(long len) throws ParseErrorException {
        ByteArrayDataOutput bo = ByteStreams.newDataOutput();
        for (long i = 0; i < len; i++) {
            try {
                bo.write(consumeByte());
            } catch (EOFException e) {
                throw new ParseErrorException();
            }
        }
        return new Slice(bo.toByteArray());
    }

    private long consumeCount() throws ParseErrorException, EOFException {
        expectByte((byte) '*');
        try {
            long count = consumePositiveLong();
            expectByte((byte) '\n');
            return count;
        } catch (EOFException e) {
            throw new ParseErrorException();
        }
    }

    private Slice consumeParameter() throws ParseErrorException {
        try {
            expectByte((byte) '$');
            long len = consumePositiveLong();
            expectByte((byte) '\n');
            Slice para = consumeSlice(len);
            expectByte((byte) '\r');
            expectByte((byte) '\n');
            return para;
        } catch (EOFException e) {
            throw new ParseErrorException();
        }
    }

    public static RedisCommand parse(InputStream messageInput) throws ParseErrorException, EOFException {
        Preconditions.checkNotNull(messageInput);

        RedisCommandParser parser = new RedisCommandParser(messageInput);
        long count = parser.consumeCount();
        if (count == 0) {
            throw new ParseErrorException();
        }
        RedisCommand command = new RedisCommand();
        for (long i = 0; i < count; i++) {
            command.addParameter(parser.consumeParameter());
        }
        return command;
    }
}
