package com.github.zxl0714.redismock.pattern.token;

import com.github.zxl0714.redismock.pattern.TokenType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kael.
 */
public class EnumericToken extends AbstractToken {

    protected byte[] expr;

    public EnumericToken(byte[] expr) {
        List<Byte> list = new ArrayList<Byte>();
        for(int i = 0; i < expr.length; i++) {
            byte b = expr[i];
            if (b == '\\') {
                if (i == expr.length - 1) {
                    list.add(b);
                } else {
                    list.add(expr[++i]);
                }
            } else if (b == '-') {
                if (i == 0) {
                    list.add(b);
                } else {
                    byte start = expr[i - 1];
                    byte end;
                    if (i == expr.length - 1) {
                        end = Byte.MAX_VALUE;
                    } else {
                        end = expr[++i];
                    }
                    if (start > end) {
                        byte t = start;
                        start = end;
                        end = t;
                    }
                    if (start < Byte.MAX_VALUE) {
                        for(byte j = (byte) (start + 1); j <= end; j++) {
                            list.add(j);
                        }
                    }
                }
            } else {
                list.add(b);
            }
        }
        this.expr = new byte[list.size()];
        for(int i = 0; i < this.expr.length; i++) {
            this.expr[i] = list.get(i);
        }
    }

    @Override
    public TokenType getType() {
        return TokenType.ENUMERIC;
    }

    @Override
    public boolean eatFirstMatch(byte[] bytes, int start) {
        if (start >= bytes.length) {
            throw new ArrayIndexOutOfBoundsException("byte array length " + bytes.length + ", request index: " + start);
        }
        if (eated.length == 1) {
            return false;
        }
        byte b = bytes[start];
        for(byte b1 : expr) {
            if (b1 == b) {
                eated = new byte[]{b};
                eatAtPosition = start;
                return true;
            }
        }
        return false;
    }


}
