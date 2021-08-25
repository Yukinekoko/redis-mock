package com.github.zxl0714.redismock.pattern.token;

import com.github.zxl0714.redismock.pattern.TokenType;

/**
 * @author kael.
 */
public class AsteriskToken extends AbstractToken {

    private boolean first = true;

    @Override
    public TokenType getType() {
        return TokenType.ASTERISK;
    }

    @Override
    public boolean eatFirstMatch(byte[] bytes, int start) {
        if (start >= bytes.length) {
            throw new ArrayIndexOutOfBoundsException("byte array length " + bytes.length + ", request index: " + start);
        }
        if (first) {
            first = false;
            eatAtPosition = start - 1;
            return true;
        }
        byte[] n = new byte[eated.length + 1];
        System.arraycopy(eated, 0, n, 0, eated.length);
        eatAtPosition++;
        n[n.length - 1] = bytes[eatAtPosition];
        eated = n;
        return true;
    }

    @Override
    public void clearEated() {

    }
}
