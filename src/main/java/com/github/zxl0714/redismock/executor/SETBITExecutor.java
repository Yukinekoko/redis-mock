package com.github.zxl0714.redismock.executor;

import com.github.zxl0714.redismock.RedisBase;
import com.github.zxl0714.redismock.Response;
import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.expecptions.BaseException;
import com.github.zxl0714.redismock.expecptions.WrongValueTypeException;

import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

import static com.github.zxl0714.redismock.Utils.checkArgumentsNumberEquals;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/26
 */
public class SETBITExecutor extends AbstractExecutor {

    @Override
    public Slice execute(List<Slice> params, RedisBase base, Socket socket) throws BaseException, IOException {
        checkArgumentsNumberEquals(params, 3);

        Slice value = base.rawGet(params.get(0));
        byte bit;
        try {
            bit = Byte.parseByte(params.get(2).toString());
            if (bit != 0 && bit != 1) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR bit is not an integer or out of range");
        }
        int pos;
        try {
            pos = Integer.parseInt(params.get(1).toString());
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR bit offset is not an integer or out of range");
        }
        if (pos < 0) {
            throw new WrongValueTypeException("ERR bit offset is not an integer or out of range");
        }
        if (value == null) {
            byte[] data = new byte[pos / 8 + 1];
            Arrays.fill(data, (byte) 0);
            data[pos / 8] = (byte) (bit << (pos % 8));
            base.rawPut(params.get(0), new Slice(data), -1L);
            return Response.integer(0L);
        }
        long original;
        if (pos / 8 >= value.length()) {
            byte[] data = new byte[pos / 8 + 1];
            Arrays.fill(data, (byte) 0);
            for(int i = 0; i < value.length(); i++) {
                data[i] = value.data()[i];
            }
            data[pos / 8] = (byte) (bit << (pos % 8));
            original = 0;
            base.rawPut(params.get(0), new Slice(data), -1L);
        } else {
            byte[] data = value.data();
            if ((data[pos / 8] & (1 << (pos % 8))) != 0) {
                original = 1;
            } else {
                original = 0;
            }
            data[pos / 8] |= (byte) (1 << (pos % 8));
            data[pos / 8] &= (byte) (bit << (pos % 8));
            base.rawPut(params.get(0), new Slice(data), -1L);
        }
        return Response.integer(original);

    }
}
