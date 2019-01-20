package com.github.zxl0714.redismock.pattern;

/**
 * @author kael.
 */
public interface Token extends Cloneable{
    
    TokenType getType();
    
    boolean eatFirstMatch(byte[] bytes, int start);
    int eatAtPosition();
    void clearEated();
    
    Token getNextToken();
    void setNextToken(Token token);
    
    Token clone();
}
