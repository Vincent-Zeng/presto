package com.facebook.presto.sql.parser;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.annotations.VisibleForTesting;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.BufferedTreeNodeStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.TreeNodeStream;

public final class SqlParser
{
    private SqlParser() {}

    public static Statement createStatement(String sql)
    {
        return createStatement( // zeng: 从树中获得statement规则节点
                parseStatement(sql) // zeng: 获得singleStatement语法分析树
        );
    }

    public static Expression createExpression(String expression)
    {
        return createExpression(parseExpression(expression));
    }

    @VisibleForTesting
    static Statement createStatement(CommonTree tree)
    {
        TreeNodeStream stream = new BufferedTreeNodeStream(tree);
        StatementBuilder builder = new StatementBuilder(stream);
        try {
            return builder.statement().value;
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    private static Expression createExpression(CommonTree tree)
    {
        TreeNodeStream stream = new BufferedTreeNodeStream(tree);
        StatementBuilder builder = new StatementBuilder(stream);
        try {
            return builder.expr().value;
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    // zeng: sql 生成 语法分析树
    @VisibleForTesting
    static CommonTree parseStatement(String sql)
    {
        try {
            // zeng: singleStatement子树
            return (CommonTree) getParser(sql).singleStatement().getTree();
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    private static CommonTree parseExpression(String expression)
    {
        try {
            return (CommonTree) getParser(expression).singleExpression().getTree();
        }
        catch (RecognitionException e) {
            throw new AssertionError(e); // RecognitionException is not thrown
        }
    }

    private static StatementParser getParser(String sql)
    {
        // zeng: 字符流
        CharStream stream = new CaseInsensitiveStream(new ANTLRStringStream(sql));
        // zeng: 词法分析器
        StatementLexer lexer = new StatementLexer(stream);
        // zeng: 词法符号流
        TokenStream tokenStream = new CommonTokenStream(lexer);
        // zeng: 语法分析器
        return new StatementParser(tokenStream);
    }
}
