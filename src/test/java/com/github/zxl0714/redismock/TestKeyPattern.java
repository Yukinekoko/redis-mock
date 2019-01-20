package com.github.zxl0714.redismock;

import com.github.zxl0714.redismock.pattern.KeyPattern;
import com.github.zxl0714.redismock.pattern.Token;
import com.github.zxl0714.redismock.pattern.TokenType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author kael.
 */
public class TestKeyPattern {
    @Test
    public void testPattern(){
        KeyPattern pattern1 = new KeyPattern(new Slice("*abc*"));
        KeyPattern pattern2 = new KeyPattern(new Slice("?abc?"));
        KeyPattern pattern3 = new KeyPattern(new Slice("[aef]abc[iop]"));
        KeyPattern pattern4 = new KeyPattern(new Slice("[aef]a*b?c[iop]"));
        KeyPattern pattern5 = new KeyPattern(new Slice("[aef]a*bcd*ef[iop]"));
        KeyPattern pattern6 = new KeyPattern(new Slice("a\\?c"));
        KeyPattern pattern7 = new KeyPattern(new Slice("a[a-d]c"));
        KeyPattern pattern8 = new KeyPattern(new Slice("a[d-a]c"));
        KeyPattern pattern9 = new KeyPattern(new Slice("a[d\\-a]c"));
        KeyPattern pattern10 = new KeyPattern(new Slice("a[d\\*a]c"));
        
        Assert.assertTrue(pattern1.match(new Slice("123abc456")));
        Assert.assertFalse(pattern1.match(new Slice("123aac456")));
        
        Assert.assertTrue(pattern2.match(new Slice("3abc5")));
        Assert.assertFalse(pattern2.match(new Slice("13abc5")));

        Assert.assertTrue(pattern3.match(new Slice("eabco")));
        Assert.assertFalse(pattern3.match(new Slice("eabc")));

        Assert.assertTrue(pattern4.match(new Slice("eababaco")));
        Assert.assertFalse(pattern4.match(new Slice("eabaaco")));

        Assert.assertTrue(pattern5.match(new Slice("eavvbcddevefp")));
        Assert.assertFalse(pattern5.match(new Slice("eavvbcddevefa")));

        Assert.assertTrue(pattern6.match(new Slice("a?c")));
        Assert.assertTrue(pattern7.match(new Slice("acc")));
        Assert.assertTrue(pattern8.match(new Slice("acc")));
        Assert.assertTrue(pattern9.match(new Slice("a-c")));
        Assert.assertTrue(pattern10.match(new Slice("a*c")));
    }
    @Test
    public void testAstResolve(){
        KeyPattern pattern = new KeyPattern(new Slice("*abc*"));
        List<Token> ast = pattern.getAst();
        Assert.assertEquals(3, ast.size());
        Assert.assertEquals(TokenType.ASTERISK, ast.get(0).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(1).getType());
        Assert.assertEquals(TokenType.ASTERISK, ast.get(2).getType());

        pattern = new KeyPattern(new Slice("?abc?"));
        ast = pattern.getAst();
        Assert.assertEquals(3, ast.size());
        Assert.assertEquals(TokenType.QUESTION_MARK, ast.get(0).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(1).getType());
        Assert.assertEquals(TokenType.QUESTION_MARK, ast.get(2).getType());

        pattern = new KeyPattern(new Slice("[aef]abc[iop]"));
        ast = pattern.getAst();
        Assert.assertEquals(3, ast.size());
        Assert.assertEquals(TokenType.ENUMERIC, ast.get(0).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(1).getType());
        Assert.assertEquals(TokenType.ENUMERIC, ast.get(2).getType());

        pattern = new KeyPattern(new Slice("[aef]a*b?c[iop]"));
        ast = pattern.getAst();
        Assert.assertEquals(7, ast.size());
        Assert.assertEquals(TokenType.ENUMERIC, ast.get(0).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(1).getType());
        Assert.assertEquals(TokenType.ASTERISK, ast.get(2).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(3).getType());
        Assert.assertEquals(TokenType.QUESTION_MARK, ast.get(4).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(5).getType());
        Assert.assertEquals(TokenType.ENUMERIC, ast.get(6).getType());

        pattern = new KeyPattern(new Slice("[aef]a*bcd*ef[iop]"));
        ast = pattern.getAst();
        Assert.assertEquals(7, ast.size());
        Assert.assertEquals(TokenType.ENUMERIC, ast.get(0).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(1).getType());
        Assert.assertEquals(TokenType.ASTERISK, ast.get(2).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(3).getType());
        Assert.assertEquals(TokenType.ASTERISK, ast.get(4).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(5).getType());
        Assert.assertEquals(TokenType.ENUMERIC, ast.get(6).getType());

        pattern = new KeyPattern(new Slice("[a\\*ef]a\\*bcd*ef[i\\?op]"));
        ast = pattern.getAst();
        Assert.assertEquals(5, ast.size());
        Assert.assertEquals(TokenType.ENUMERIC, ast.get(0).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(1).getType());
        Assert.assertEquals(TokenType.ASTERISK, ast.get(2).getType());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(3).getType());
        Assert.assertEquals(TokenType.ENUMERIC, ast.get(4).getType());

        pattern = new KeyPattern(new Slice("\\\\a"));
        ast = pattern.getAst();
        Assert.assertEquals(1, ast.size());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(0).getType());

        pattern = new KeyPattern(new Slice("\\a\\"));
        ast = pattern.getAst();
        Assert.assertEquals(1, ast.size());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(0).getType());

        pattern = new KeyPattern(new Slice("a[\\"));
        ast = pattern.getAst();
        Assert.assertEquals(2, ast.size());
        Assert.assertEquals(TokenType.IDENTIFIER, ast.get(0).getType());
        Assert.assertEquals(TokenType.ENUMERIC, ast.get(1).getType());
    }
    
}
