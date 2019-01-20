package com.github.zxl0714.redismock.pattern.token;

import com.github.zxl0714.redismock.pattern.Token;

/**
 * @author kael.
 */
public abstract class AbstractToken implements Token {

    protected Token  nextToken;
    protected byte[] eated = new byte[0];
    protected int eatAtPosition = 0;
    
    @Override
    public Token getNextToken() {
        return nextToken;
    }

    public void setNextToken(Token nextToken) {
        this.nextToken = nextToken;
    }

    @Override
    public int eatAtPosition() {
        return eatAtPosition;
    }

    @Override
    public void clearEated() {
        eated = new byte[0];
    }

    @Override
    public Token clone() {
        try {
            return (Token) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }
}
