package com.github.zxl0714.redismock.pattern.token;

import com.github.zxl0714.redismock.pattern.TokenType;

/**
 * @author kael.
 */
public class QuestionMaskToken extends AbstractToken {

    @Override
    public TokenType getType() {
        return TokenType.QUESTION_MARK;
    }

    @Override
    public boolean eatFirstMatch(byte[] bytes, int start) {
        if(start >= bytes.length){
            throw new ArrayIndexOutOfBoundsException("byte array length " + bytes.length + ", request index: " + start);
        }
        if(eated.length == 1){
            return false;
        }
        eated = new byte[]{bytes[start]};
        eatAtPosition = start;
        return true;
    }
}
