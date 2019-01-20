package com.github.zxl0714.redismock.pattern.token;

import com.github.zxl0714.redismock.pattern.TokenType;

/**
 * @author kael.
 */
public class AsteriskToken extends AbstractToken {
    @Override
    public TokenType getType() {
        return TokenType.ASTERISK;
    }

    @Override
    public boolean eatFirstMatch(byte[] bytes, int start) {
        if (start >= bytes.length) {
            throw new ArrayIndexOutOfBoundsException("byte array length " + bytes.length + ", request index: " + start);
        }
        byte[] n = new byte[eated.length + 1];
        System.arraycopy(eated, 0, n, 0, eated.length);
        n[n.length - 1] = bytes[start];
        eated = n;
        eatAtPosition = start;
        return true;
    }

    @Override
    public void clearEated() {
        
    }
}
