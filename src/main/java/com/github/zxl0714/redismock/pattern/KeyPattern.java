package com.github.zxl0714.redismock.pattern;

import com.github.zxl0714.redismock.Slice;
import com.github.zxl0714.redismock.pattern.token.AsteriskToken;
import com.github.zxl0714.redismock.pattern.token.EnumericToken;
import com.github.zxl0714.redismock.pattern.token.IdentifierToken;
import com.github.zxl0714.redismock.pattern.token.QuestionMaskToken;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author kael.
 */
public class KeyPattern {
    protected Slice       pattern;
    protected List<Token> ast;

    public KeyPattern(Slice pattern) {
        if (null == pattern) {
            throw new NullPointerException("pattern");
        }
        this.pattern = pattern;
        resolve();
    }

    protected void resolve() {
        List<Token> ast = new ArrayList<Token>();
        int start = 0;
        int end = 0;
        boolean inEnumric = false;
        boolean inIdentifier = false;
        byte[] data = pattern.data();
        for(int i = 0; i < data.length; i++) {
            switch (data[end]) {
                case '?':
                    if (inIdentifier) {
                        inIdentifier = false;
                        byte[] identifier = new byte[end - start];
                        System.arraycopy(data, start, identifier, 0, identifier.length);
                        ast.add(new IdentifierToken(identifier));
                        ast.add(new QuestionMaskToken());
                        end++;
                        start = end;
                    } else if (inEnumric) {
                        end++;
                    } else {
                        ast.add(new QuestionMaskToken());
                        end++;
                        start = end;
                    }
                    break;
                case '*':
                    if (inIdentifier) {
                        inIdentifier = false;
                        byte[] identifier = new byte[end - start];
                        System.arraycopy(data, start, identifier, 0, identifier.length);
                        ast.add(new IdentifierToken(identifier));
                        ast.add(new AsteriskToken());
                        end++;
                        start = end;
                    } else if (inEnumric) {
                        end++;
                    } else {
                        ast.add(new AsteriskToken());
                        end++;
                        start = end;
                    }
                    break;
                case '[':
                    if (inIdentifier) {
                        inIdentifier = false;
                        byte[] identifier = new byte[end - start];
                        System.arraycopy(data, start, identifier, 0, identifier.length);
                        ast.add(new IdentifierToken(identifier));
                        end++;
                        start = end;
                        inEnumric = true;
                    } else if (inEnumric) {
                        end++;
                    } else {
                        inEnumric = true;
                        end++;
                        start = end;
                    }
                    break;
                case ']':
                    if (inIdentifier) {
                        end++;
                    } else if (inEnumric) {
                        inEnumric = false;
                        byte[] expr = new byte[end - start];
                        System.arraycopy(data, start, expr, 0, expr.length);
                        ast.add(new EnumericToken(expr));
                        end++;
                        start = end;
                    } else {
                        inIdentifier = true;
                        end++;
                        start = end;
                    }
                    break;
                case '\\':
                    end += 2;
                    i++;
                    if (i >= data.length) {
                        if (inIdentifier) {
                            byte[] identifier = new byte[end - 1 - start];
                            System.arraycopy(data, start, identifier, 0, identifier.length);
                            ast.add(new IdentifierToken(identifier));
                        } else if (inEnumric) {
                            byte[] expr = new byte[end - 1 - start];
                            System.arraycopy(data, start, expr, 0, expr.length);
                            ast.add(new EnumericToken(expr));
                        } else {
                            ast.add(new IdentifierToken(new byte[]{'\\'}));
                        }
                        end--;
                        start = end;
                    } else if (inEnumric) {

                    } else {
                        inIdentifier = true;
                    }
                    break;
                default:
                    if (inIdentifier) {
                        end++;
                    } else if (inEnumric) {
                        end++;
                    } else {
                        inIdentifier = true;
                        start = end;
                        end++;
                    }
                    break;
            }
        }
        if (start != end) {
            if (inEnumric) {
                byte[] expr = new byte[data.length - start];
                System.arraycopy(data, start, expr, 0, expr.length);
                ast.add(new EnumericToken(expr));
            } else {
                byte[] identifier = new byte[data.length - start];
                System.arraycopy(data, start, identifier, 0, identifier.length);
                ast.add(new IdentifierToken(identifier));
            }
        }
        for(int i = 0; i < ast.size() - 1; i++) {
            ast.get(i).setNextToken(ast.get(i + 1));
        }
        this.ast = ast;
    }

    public List<Token> getAst() {
        return Collections.unmodifiableList(ast);
    }

    protected List<Token> cloneAst() {
        List<Token> clone = new ArrayList<Token>(getAst().size());
        for(Token token : getAst()) {
            clone.add(token.clone());
        }
        return clone;
    }

    public boolean match(Slice key) {
        List<Token> ast = cloneAst();
        int start = 0;
        int index = 0;
        while (true) {
            if(index < 0){
                return false;
            }
            if (index >= ast.size()) {
                return start == key.length();
            }
            if (start >= key.length()) {
                return false;
            }
            Token token = ast.get(index);
            if (!token.eatFirstMatch(key.data(), start)) {
                index--;
                token.clearEated();
                continue;
            }
            start = token.eatAtPosition()+1;
            if (start >= key.length() && index == ast.size() - 1) {
                return true;
            }
            if (index < ast.size() - 1) {
                index++;
            }
        }
    }
}
