package com.github.zxl0714.redismock.pattern.token;

import com.github.zxl0714.redismock.pattern.TokenType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kael.
 */
public class IdentifierToken extends AbstractToken {

    protected byte[] data;

    protected byte[] eated = new byte[0];

    public IdentifierToken(byte[] data) {
        List<Byte> list = new ArrayList<Byte>(data.length);
        for(int i = 0; i < data.length; i++) {
            byte b = data[i];
            if(b == '\\'){
                if (i == data.length-1){
                    list.add(b);
                }else {
                    list.add(data[++i]);
                }
            }else {
                list.add(b);
            }
        }
        this.data = new byte[list.size()];
        for(int i = 0; i < this.data.length; i ++){
            this.data[i] = list.get(i);
        }
    }

    @Override
    public TokenType getType() {
        return TokenType.IDENTIFIER;
    }

    @Override
    public boolean eatFirstMatch(byte[] bytes, int start) {
        if (start >= bytes.length) {
            throw new ArrayIndexOutOfBoundsException("byte array length " + bytes.length + ", request index: " + start);
        }
        if (eated.length == data.length) {
            return false;
        }
        int subLen = bytes.length - start;
        if (data.length > subLen) {
            return false;
        }
        for(int i = 0; i < data.length; i++) {
            if (data[i] != bytes[start + i]) {
                return false;
            }
        }
        eated = new byte[data.length];
        System.arraycopy(bytes, start, eated, 0, eated.length);
        eatAtPosition = start + data.length - 1;
        return true;
    }

}
