package com.github.zxl0714.redismock.lua;

import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

/**
 * @author snowmeow(yuki754685421 @ 163.com)
 * @date 2021/7/28
 */
class StructHandler {

    private static final int MAX_INT_SIZE = 32;

    private final StringBuilder response;

    private final char[] expression;

    private final Varargs args;

    private int expIndex;

    private int argKey;

    private boolean bigEndian;

    private int align;

    public StructHandler(Varargs args) {
        expression = args.arg1().checkjstring().toCharArray();
        response = new StringBuilder();
        expIndex = 0;
        argKey = 2;
        this.args = args;
        bigEndian = false;
        align = 1;
    }

    public String pack() {

        int totalSize = 0;

        while (expIndex < expression.length) {
            char cur = expression[expIndex];
            int size = typeSize(cur);
            int toAlign = getToAlign(totalSize, size, cur);
            totalSize += toAlign;
            while (toAlign-- > 0) {
                response.append('\0');
            }

            if (cur == 'b' || cur == 'B' || cur == 'h' || cur == 'H' || cur == 'I' ||
                cur == 'l' || cur == 'L' || cur == 'T' || cur == 'i') {
                putInteger(size);
            } else if (cur == 'x') {
                response.append((byte) 0x00);
            } else if (cur == 'f') {
                float f = (float) args.arg(argKey).checkdouble();
                if (bigEndian) {
                    int intBits = Float.floatToIntBits(f);
                    intBits = Integer.reverseBytes(intBits);
                    f = Float.intBitsToFloat(intBits);
                }
                response.append(f);
            } else if (cur == 'd') {
                double d = args.arg(argKey).checkdouble();
                if (bigEndian) {
                    long longBits = Double.doubleToLongBits(d);
                    longBits = Long.reverseBytes(longBits);
                    d = Double.longBitsToDouble(longBits);
                }
                response.append(d);
            } else if (cur == 'c' || cur == 's') {
                putString(size);
            } else {
                controlOptions(cur);
                expIndex++;
                totalSize += size;
                continue;
            }
            totalSize += size;
            argKey++;
            expIndex++;
        }
        return response.toString();
    }

    // TODO : (snowmeow:2021/8/24) 待完善
    public Varargs unpack() {

        int totalSize = 0;
        String data = args.arg(2).checkjstring();
        int ld = data.length();

        while (expIndex < expression.length) {
            char cur = expression[expIndex];
            int size = typeSize(cur);
            totalSize += getToAlign(totalSize, size, cur);
            if (size <= ld - totalSize) {
                throw new LuaError("data string too short");
            }

            if (cur == 'b' || cur == 'B' || cur == 'h' || cur == 'H' || cur == 'I' ||
                cur == 'l' || cur == 'L' || cur == 'T' || cur == 'i') {

            } else if (cur == 'x') {

            } else if (cur == 'f') {

            } else if (cur == 'd') {

            } else if (cur == 'c' || cur == 's') {

            } else {

            }

        }

        return LuaValue.NIL;
    }

    public int size() {
        return 0;
    }

    private int getToAlign(int totalSize, int size, char c) {
        if (size == 0 || c == 'c') {
            return 0;
        }
        if (size > align) {
            size = align;
        }
        return (size - (totalSize & (size - 1))) & (size - 1);
    }

    private void controlOptions(char c) {
        if (c == ' ') {
            // return;
        } else if (c == '>') {
            bigEndian = true;
        } else if (c == '<') {
            bigEndian = false;
        } else if (c == '!') {
            int size = getSizeNumber(4); // sizeof(int)
            if (size % 2 != 0) {
                throw new LuaError(String.format("alignment %d is not a power of 2", size));
            }
            align = size;
        } else {
            throw new LuaError(String.format("invalid format option '%c'", c));
        }
    }

    private void putString(int size) {
        String value = args.arg(argKey).checkjstring();
        if (value.length() == 0) {
            throw new LuaError("string too short");
        }
        if (size == 0) {
            size = value.length();
        }
        response.append(value, 0, size);
        if (expression[expIndex] == 's') {
            response.append('\0');
            // size++;
        }
    }

    private void putInteger(int size) {
        byte[] buff = new byte[MAX_INT_SIZE];

        long value = args.arg(argKey).checklong();

        if (bigEndian) {
            for (int i = size - 1; i >= 0; i--) {
                buff[i] = (byte) (value & 0xff);
                value >>>= 8;
            }
        } else {
            for (int i = 0; i < size; i++) {
                buff[i] = (byte) (value & 0xff);
                value >>>= 8;
            }
        }
        response.append(new String(buff, 0, size));

    }

    /**
     * TODO : (snowmeow:2021/8/24) 待解决
     * @param data 打包数据
     * @param pos 起始索引
     * @param isSigned 是否无符号？
     * @param size 数据长度
     * */
    private long getInteger(String data, int pos, boolean isSigned, int size) {
        byte[] buff = data.getBytes();
        long num = 0;
        if (bigEndian) {
            for (int i = pos; i < pos + size; i++) {
                num <<= 8;
                num |= buff[i];
            }
        } else {
            for (int i = pos + size - 1; i >= pos; i--) {
                num <<= 8;
                num |= buff[i];
            }
        }
        return 0;
    }

    private int typeSize(char type) {
        if (type == 'b' || type == 'B' || type == 'x') {
            return 1;
        } else if (type == 'h' || type == 'H') {
            return 2;
        } else if (type == 'f') {
            return 4;
        } else if (type == 'l' || type == 'L' || type == 'd' || type == 'T') {
            return 8;
        } else if (type == 'i') {
            int size = getSizeNumber(4);
            if (size > MAX_INT_SIZE) {
                throw new LuaError(String.format("integral size %d is larger than limit of %d", size, MAX_INT_SIZE));
            }
            return size;
        } else if (type == 'c') {
            return getSizeNumber(1);
        } else {
            // s -- 0
            return 0;
        }
    }

    private int getSizeNumber(int defaultSize) {
        int curIndex = expIndex + 1;
        if (curIndex >= expression.length || !isNumber(expression[curIndex])) {
            return defaultSize;
        }
        int size = 0;
        while (curIndex < expression.length && isNumber(expression[curIndex])) {
            size = size * 10 + (expression[curIndex++] - '0');
        }
        expIndex = curIndex - 1;
        return size;
    }

    private boolean isNumber(char c) {
        return c >= '0' && c <= '9';
    }

}
